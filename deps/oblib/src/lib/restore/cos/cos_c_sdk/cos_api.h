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

#ifndef LIBCOS_API_H
#define LIBCOS_API_H

#include "cos_sys_util.h"
#include "cos_string.h"
#include "cos_status.h"
#include "cos_define.h"
#include "cos_utility.h"
#include <stdint.h>

COS_CPP_START

/*
 * @brief  get cos service
 * @param[in]   options       the cos request options
 * @param[in]   params        a switch of region or all region for get service request
 * @param[out]  params        output params for get service response,
                              including owner msg and buckets list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_service(const cos_request_options_t *options,
                                cos_get_service_params_t *params,
                                cos_table_t **resp_headers);

/*
 * @brief  get cos service
 * @param[in]   options       the cos request options
 * @param[in]   params        a switch of region or all region for get service request
 * @param[out]  params        output params for get service response,
 *                            including owner msg and buckets list
 * @param[in]   header        the headers for request.
 *
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_get_service(const cos_request_options_t *options,
                                cos_get_service_params_t *params,
                                cos_table_t *header,
                                cos_table_t **resp_headers);


/*
 * @brief  head cos bucket
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_head_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t **resp_headers);

/*
 * @brief  head cos bucket
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   header        the headers for request.
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_head_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t *header,
                                cos_table_t **resp_headers);


/*
 * @brief  create cos bucket
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   cos_acl       the cos bucket acl
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_create_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_acl_e cos_acl,
                                cos_table_t **resp_headers);

/*
 * @brief  create cos bucket
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   cos_acl       the cos bucket acl
 * @param[in]   headers       the headers for request
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_create_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_acl_e cos_acl,
                                cos_table_t *headers,
                                cos_table_t **resp_headers);

/*
 * @brief  delete cos bucket
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t **resp_headers);

/*
 * @brief  delete cos bucket
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   headers       the headers for request
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_delete_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t *headers,
                                cos_table_t **resp_headers);



/*
 * @brief  put cos bucket acl
 * @param[in]   options         the cos request options
 * @param[in]   bucket          the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   cos_acl         the cos bucket acl
 * @param[in]   grant_read      account granted read
 * @param[in]   grant_write     account granted write
 * @param[in]   grant_full_ctrl account granted full control
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_acl_e cos_acl,
                                 const cos_string_t *grant_read,
                                 const cos_string_t *grant_write,
                                 const cos_string_t *grant_full_ctrl,
                                 cos_table_t **resp_headers);

/*
 * @brief  get cos bucket acl
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  acl_param     the cos bucket acl param
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_acl_params_t *acl_param,
                                 cos_table_t **resp_headers);

/*
 * @brief  put cos bucket cors
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   cors_rule_list      the cos bucket cors list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_cors(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *cors_rule_list,
                                       cos_table_t **resp_headers);

/*
 * @brief  get cos bucket cors
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  cors_rule_list      the cos bucket cors list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_cors(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *cors_rule_list,
                                       cos_table_t **resp_headers);

/*
 * @brief  delete cos bucket cors
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_bucket_cors(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          cos_table_t **resp_headers);


/*
 * @brief  put cos bucket versioning
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   versioning          the cos bucket versioning parameter
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_versioning
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_versioning_content_t *versioning,
    cos_table_t **resp_headers
);

/*
 * @brief  get cos bucket versioning
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  versioning          the cos bucket versioning parameter
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_versioning
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_versioning_content_t *versioning,
    cos_table_t **resp_headers
);

/*
 * @brief  put cos bucket replication
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   replication_param   the cos bucket replication parameter
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_replication
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_replication_params_t *replication_param,
    cos_table_t **resp_headers
);

/*
 * @brief  get cos bucket replication
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  replication_param   the cos bucket replication parameter
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_replication
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_replication_params_t *replication_param,
    cos_table_t **resp_headers
);

/*
 * @brief  delete cos bucket replication
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_bucket_replication
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_table_t **resp_headers
);

/*
 * @brief  put cos bucket lifecycle
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   lifecycle_rule_list the cos bucket lifecycle list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_lifecycle(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *lifecycle_rule_list,
                                       cos_table_t **resp_headers);

/*
 * @brief  get cos bucket lifecycle
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  lifecycle_rule_list the cos bucket lifecycle list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_lifecycle(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *lifecycle_rule_list,
                                       cos_table_t **resp_headers);

/*
 * @brief  delete cos bucket lifecycle
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_bucket_lifecycle(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          cos_table_t **resp_headers);

/*
 * @brief  put cos bucket website
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   website_params  the cos bucket website configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_website(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_website_params_t *website_params,
                                    cos_table_t **resp_header);

/*
 * @brief  get cos bucket website
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  website_params  the cos bucket website configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_website(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_website_params_t *website_params,
                                    cos_table_t **resp_header);

/*
 * @brief  del cos bucket website
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_bucket_website(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_table_t **resp_header);

/*
 * @brief  put cos bucket domain
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   domain_params  the cos bucket domain configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_domain(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_domain_params_t *domain_params,
                                    cos_table_t **resp_header);

/*
 * @brief  get cos bucket domain
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  domain_params  the cos bucket domain configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_domain(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_domain_params_t *domain_params,
                                    cos_table_t **resp_header);

/*
 * @brief  put cos bucket logging
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   logging_params  the cos bucket logging configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_logging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_logging_params_t *logging_params,
                                    cos_table_t **resp_headers);
/*
 * @brief  get cos bucket logging
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  logging_params  the cos bucket logging configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_logging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_logging_params_t *logging_params,
                                    cos_table_t **resp_headers);
/*
 * @brief  put cos bucket inventory
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   inventory_params  the cos bucket inventory configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */

cos_status_t *cos_put_bucket_inventory(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_inventory_params_t *inventory_params,
                                    cos_table_t **resp_headers);
/*
 * @brief  get cos bucket inventory
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  inventory_params  the cos bucket inventory configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_bucket_inventory(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_inventory_params_t *inventory_params,
                                    cos_table_t **resp_headers);
/*
 * @brief  list cos bucket inventory
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  inventory_params  the cos bucket inventory configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_list_bucket_inventory(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_list_inventory_params_t *inventory_params,
                                    cos_table_t **resp_headers);
/*
 * @brief  delete cos bucket inventory
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   id            the cos bucket inventory configuration id
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_bucket_inventory(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    const cos_string_t *id,
                                    cos_table_t **resp_headers);
/*
 * @brief  put cos bucket tagging
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   tagging_params  the cos bucket tagging configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_bucket_tagging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_tagging_params_t *tagging_params,
                                    cos_table_t **resp_headers);
/*
 * @brief  get cos bucket tagging
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  tagging_params  the cos bucket tagging configuration
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */

cos_status_t *cos_get_bucket_tagging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_tagging_params_t *tagging_params,
                                    cos_table_t **resp_headers);
/*
 * @brief  delete cos bucket tagging
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[out]  resp_headers    cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_bucket_tagging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_table_t **resp_headers);


cos_status_t *cos_put_bucket_intelligenttiering(const cos_request_options_t *options,
                                                const cos_string_t *bucket,
                                                cos_intelligenttiering_params_t *params,
                                                cos_table_t **resp_headers);

cos_status_t *cos_get_bucket_intelligenttiering(const cos_request_options_t *options,
                                                const cos_string_t *bucket,
                                                cos_intelligenttiering_params_t *params,
                                                cos_table_t **resp_headers);


cos_status_t *cos_put_object_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 const cos_string_t *object,
                                 cos_acl_e cos_acl,
                                 const cos_string_t *grant_read,
                                 const cos_string_t *grant_write,
                                 const cos_string_t *grant_full_ctrl,
                                 cos_table_t **resp_headers);

cos_status_t *cos_get_object_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 const cos_string_t *object,
                                 cos_acl_params_t *acl_param,
                                 cos_table_t **resp_headers);


/*
 * @brief  list cos objects
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   params        input params for list object request,
                              including prefix, marker, delimiter, max_ret
 * @param[out]  params        output params for list object response,
                              including truncated, next_marker, obje list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_list_object(const cos_request_options_t *options,
                              const cos_string_t *bucket,
                              cos_list_object_params_t *params,
                              cos_table_t **resp_headers);

/*
 * @brief  list cos objects
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   headers       the headers for request
 * @param[in]   params        input params for list object request,
                              including prefix, marker, delimiter, max_ret
 * @param[out]  params        output params for list object response,
                              including truncated, next_marker, obje list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_list_object(const cos_request_options_t *options,
                              const cos_string_t *bucket,
                              cos_table_t *headers,
                              cos_list_object_params_t *params,
                              cos_table_t **resp_headers);

/*
 * @brief  put cos object from buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   buffer              the buffer containing object content
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_object_from_buffer(const cos_request_options_t *options,
                                         const cos_string_t *bucket,
                                         const cos_string_t *object,
                                         cos_list_t *buffer,
                                         cos_table_t *headers,
                                         cos_table_t **resp_headers);

/*
 * @brief  put cos object from file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   filename            the filename to put
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_object_from_file(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       const cos_string_t *object,
                                       const cos_string_t *filename,
                                       cos_table_t *headers,
                                       cos_table_t **resp_headers);

/*
 * @brief  put cos object from buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   buffer              the buffer containing object content
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   progress_callback   the progress callback function
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  resp_body           cos server response body
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_put_object_from_buffer(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            cos_list_t *buffer,
                                            cos_table_t *headers,
                                            cos_table_t *params,
                                            cos_progress_callback progress_callback,
                                            cos_table_t **resp_headers,
                                            cos_list_t *resp_body);

/*
 * @brief  put cos object from file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   filename            the filename to put
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   progress_callback   the progress callback function
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  resp_body           cos server response body
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_put_object_from_file(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          const cos_string_t *filename,
                                          cos_table_t *headers,
                                          cos_table_t *params,
                                          cos_progress_callback progress_callback,
                                          cos_table_t **resp_headers,
                                          cos_list_t *resp_body);

/*
 * @brief  get cos object to buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[out]  buffer              the buffer containing object content
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_object_to_buffer(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       const cos_string_t *object,
                                       cos_table_t *headers,
                                       cos_table_t *params,
                                       cos_list_t *buffer,
                                       cos_table_t **resp_headers);

/*
 * @brief  get cos object to buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   progress_callback   the progress callback function
 * @param[out]  buffer              the buffer containing object content
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_get_object_to_buffer(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          cos_table_t *headers,
                                          cos_table_t *params,
                                          cos_list_t *buffer,
                                          cos_progress_callback progress_callback,
                                          cos_table_t **resp_headers);

/*
 * @brief  get cos object to file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]  filename             the filename storing object content
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_object_to_file(const cos_request_options_t *options,
                                     const cos_string_t *bucket,
                                     const cos_string_t *object,
                                     cos_table_t *headers,
                                     cos_table_t *params,
                                     cos_string_t *filename,
                                     cos_table_t **resp_headers);

/*
 * @brief  get cos object to file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   filename            the filename storing object content
 * @param[in]   progress_callback   the progress callback function
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_get_object_to_file(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        cos_table_t *headers,
                                        cos_table_t *params,
                                        cos_string_t *filename,
                                        cos_progress_callback progress_callback,
                                        cos_table_t **resp_headers);

/*
 * @brief  head cos object
 * @param[in]   options          the cos request options
 * @param[in]   bucket           the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object           the cos object name
 * @param[in]   headers          the headers for request
 * @param[out]  resp_headers     cos server response headers containing object meta
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_head_object(const cos_request_options_t *options,
                              const cos_string_t *bucket,
                              const cos_string_t *object,
                              cos_table_t *headers,
                              cos_table_t **resp_headers);

/*
 * @brief  delete cos object
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_object(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                const cos_string_t *object,
                                cos_table_t **resp_headers);

/*
 * @brief  delete cos object
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_delete_object(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                const cos_string_t *object,
                                cos_table_t *headers,
                                cos_table_t **resp_headers);

/*
 * @brief  delete cos objects
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object_list         the cos object list name
 * @param[in]   is_quiet            is quiet or verbose
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  deleted_object_list deleted object list
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_objects(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_list_t *object_list,
                                 int is_quiet,
                                 cos_table_t **resp_headers,
                                 cos_list_t *deleted_object_list);

/*
 * @brief  delete cos objects
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object_list         the cos object list name
 * @param[in]   is_quiet            is quiet or verbose
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  deleted_object_list deleted object list
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_delete_objects(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_list_t *object_list,
                                 int is_quiet,
                                 cos_table_t *headers,
                                 cos_table_t **resp_headers,
                                 cos_list_t *deleted_object_list);

/*
 * @brief  delete cos objects by prefix
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   prefix              prefix of delete objects
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_objects_by_prefix(cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *prefix);


/*
 * @brief  append cos object from buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   position            the start position append
 * @param[in]   buffer              the buffer containing object content
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_append_object_from_buffer(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            int64_t position,
                                            cos_list_t *buffer,
                                            cos_table_t *headers,
                                            cos_table_t **resp_headers);

/*
 * @brief  append cos object from buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   position            the start position append
 * @param[in]   init_crc            the initial crc value
 * @param[in]   buffer              the buffer containing object content
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   progress_callback   the progress callback function
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  resp_body           cos server response body
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_append_object_from_buffer(const cos_request_options_t *options,
                                               const cos_string_t *bucket,
                                               const cos_string_t *object,
                                               int64_t position,
                                               uint64_t init_crc,
                                               cos_list_t *buffer,
                                               cos_table_t *headers,
                                               cos_table_t *params,
                                               cos_progress_callback progress_callback,
                                               cos_table_t **resp_headers,
                                               cos_list_t *resp_body);

/*
 * @brief  append cos object from file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   position            the start position append
 * @param[in]   append_file         the file containing appending content
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_append_object_from_file(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          int64_t position,
                                          const cos_string_t *append_file,
                                          cos_table_t *headers,
                                          cos_table_t **resp_headers);

/*
 * @brief  append cos object from file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   position            the start position append
 * @param[in]   init_crc            the initial crc value
 * @param[in]   append_file         the file containing appending content
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   progress_callback   the progress callback function
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  resp_body           cos server response body
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_append_object_from_file(const cos_request_options_t *options,
                                             const cos_string_t *bucket,
                                             const cos_string_t *object,
                                             int64_t position,
                                             uint64_t init_crc,
                                             const cos_string_t *append_file,
                                             cos_table_t *headers,
                                             cos_table_t *params,
                                             cos_progress_callback progress_callback,
                                             cos_table_t **resp_headers,
                                             cos_list_t *resp_body);

/*
 * @brief  copy cos object
 * @param[in]   options             the cos request options
 * @param[in]   src_bucket          the cos copy source bucket, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   src_object          the cos source object name
 * @param[in]   src_endpoint        the cos source endpoint
 * @param[in]   dest_bucket         the cos dest bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   dest_object         the cos dest object name
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_copy_object(const cos_request_options_t *options,
                              const cos_string_t *src_bucket,
                              const cos_string_t *src_object,
                              const cos_string_t *src_endpoint,
                              const cos_string_t *dest_bucket,
                              const cos_string_t *dest_object,
                              cos_table_t *headers,
                              cos_copy_object_params_t *copy_object_param,
                              cos_table_t **resp_headers);

/*
 * @brief  copy cos object, this api support object larger than 5G using multi-thread upload
 * @param[in]   options             the cos request options
 * @param[in]   src_bucket          the cos copy source bucket, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   src_object          the cos source object name
 * @param[in]   src_endpoint        the cos source endpoint
 * @param[in]   dest_bucket         the cos dest bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   dest_object         the cos dest object name
 * @param[in]   thread_num          thread count used to copy object
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *copy
(
    cos_request_options_t *options,
    const cos_string_t *src_bucket,
    const cos_string_t *src_object,
    const cos_string_t *src_endpoint,
    const cos_string_t *dest_bucket,
    const cos_string_t *dest_object,
    int32_t thread_num
);

/*
 * @brief  post object restore from archive
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   cos_post_object_restore    the params for restore operation
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_post_object_restore(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            cos_object_restore_params_t *restore_params,
                                            cos_table_t *headers,
                                            cos_table_t *params,
                                            cos_table_t **resp_headers);

#if 0
/*
 * @brief  gen signed url for cos object api
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   expires             the end expire time for signed url
 * @param[in]   req                 the cos http request
 * @return  signed url, non-NULL success, NULL failure
 */
char *cos_gen_signed_url(const cos_request_options_t *options,
                         const cos_string_t *bucket,
                         const cos_string_t *object,
                         int64_t expires,
                         cos_http_request_t *req);

/*
 * @brief  cos put object from buffer using signed url
 * @param[in]   options             the cos request options
 * @param[in]   signed_url          the signed url for put object
 * @param[in]   buffer              the buffer containing object content
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_object_from_buffer_by_url(const cos_request_options_t *options,
                                                const cos_string_t *signed_url,
                                                cos_list_t *buffer,
                                                cos_table_t *headers,
                                                cos_table_t **resp_headers);

/*
 * @brief  cos put object from file using signed url
 * @param[in]   options             the cos request options
 * @param[in]   signed_url          the signed url for put object
 * @param[in]   filename            the filename containing object content
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_object_from_file_by_url(const cos_request_options_t *options,
                                              const cos_string_t *signed_url,
                                              cos_string_t *filename,
                                              cos_table_t *headers,
                                              cos_table_t **resp_headers);

/*
 * @brief  cos get object to buffer using signed url
 * @param[in]   options             the cos request options
 * @param[in]   signed_url          the signed url for put object
 * @param[in]   buffer              the buffer containing object content
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_object_to_buffer_by_url(const cos_request_options_t *options,
                                              const cos_string_t *signed_url,
                                              cos_table_t *headers,
                                              cos_table_t *params,
                                              cos_list_t *buffer,
                                              cos_table_t **resp_headers);

/*
 * @brief  cos get object to file using signed url
 * @param[in]   options             the cos request options
 * @param[in]   signed_url          the signed url for put object
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   filename            the filename containing object content
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_object_to_file_by_url(const cos_request_options_t *options,
                                            const cos_string_t *signed_url,
                                            cos_table_t *headers,
                                            cos_table_t *params,
                                            cos_string_t *filename,
                                            cos_table_t **resp_headers);

/*
 * @brief  cos head object using signed url
 * @param[in]   options             the cos request options
 * @param[in]   signed_url          the signed url for put object
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_head_object_by_url(const cos_request_options_t *options,
                                     const cos_string_t *signed_url,
                                     cos_table_t *headers,
                                     cos_table_t **resp_headers);
#endif

/*
 * @brief  cos init multipart upload
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_init_multipart_upload(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        cos_string_t *upload_id,
                                        cos_table_t *headers,
                                        cos_table_t **resp_headers);

/*
 * @brief  cos upload part from buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   part_num            the upload part number
 * @param[in]   buffer              the buffer containing upload part content
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_upload_part_from_buffer(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          const cos_string_t *upload_id,
                                          int part_num,
                                          cos_list_t *buffer,
                                          cos_table_t **resp_headers);

/*
 * @brief  cos upload part from buffer
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   part_num            the upload part number
 * @param[in]   buffer              the buffer containing upload part content
 * @param[in]   progress_callback   the progress callback function
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  resp_body           cos server response body
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_upload_part_from_buffer(const cos_request_options_t *options,
                                             const cos_string_t *bucket,
                                             const cos_string_t *object,
                                             const cos_string_t *upload_id,
                                             int part_num,
                                             cos_list_t *buffer,
                                             cos_progress_callback progress_callback,
                                             cos_table_t *headers,
                                             cos_table_t *params,
                                             cos_table_t **resp_headers,
                                             cos_list_t *resp_body);

/*
 * @brief  cos upload part from file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   part_num            the upload part number
 * @param[in]   upload_file         the file containing upload part content
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_upload_part_from_file(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        const cos_string_t *upload_id,
                                        int part_num,
                                        cos_upload_file_t *upload_file,
                                        cos_table_t **resp_headers);

/*
 * @brief  cos upload part from file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   part_num            the upload part number
 * @param[in]   upload_file         the file containing upload part content
 * @param[in]   progress_callback   the progress callback function
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  resp_body           cos server response body
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_upload_part_from_file(const cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *object,
                                           const cos_string_t *upload_id,
                                           int part_num,
                                           cos_upload_file_t *upload_file,
                                           cos_progress_callback progress_callback,
                                           cos_table_t *headers,
                                           cos_table_t *params,
                                           cos_table_t **resp_headers,
                                           cos_list_t *resp_body);

/*
 * @brief  cos abort multipart upload
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_abort_multipart_upload(const cos_request_options_t *options,
                                         const cos_string_t *bucket,
                                         const cos_string_t *object,
                                         cos_string_t *upload_id,
                                         cos_table_t **resp_headers);


/*
 * @brief  cos complete multipart upload
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   part_list           the uploaded part list to complete
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_complete_multipart_upload(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            const cos_string_t *upload_id,
                                            cos_list_t *part_list,
                                            cos_table_t *headers,
                                            cos_table_t **resp_headers);

/*
 * @brief  cos complete multipart upload
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   part_list           the uploaded part list to complete
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[out]  resp_headers        cos server response headers
 * @param[out]  resp_body           cos server response body
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_complete_multipart_upload(const cos_request_options_t *options,
                                               const cos_string_t *bucket,
                                               const cos_string_t *object,
                                               const cos_string_t *upload_id,
                                               cos_list_t *part_list,
                                               cos_table_t *headers,
                                               cos_table_t *params,
                                               cos_table_t **resp_headers,
                                               cos_list_t *resp_body);

/*
 * @brief  cos list upload part with specific upload_id for object
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   params              the input list upload part parameters,
                                    incluing part_number_marker, max_ret
 * @param[out]  params              the output params,
                                    including next_part_number_marker, part_list, truncated
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_list_upload_part(const cos_request_options_t *options,
                                   const cos_string_t *bucket,
                                   const cos_string_t *object,
                                   const cos_string_t *upload_id,
                                   cos_list_upload_part_params_t *params,
                                   cos_table_t **resp_headers);

/*
 * @brief  cos list multipart upload for bucket
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   params              the input list multipart upload parameters
 * @param[out]  params              the output params including next_key_marker, next_upload_id_markert, upload_list etc
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_list_multipart_upload(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_list_multipart_upload_params_t *params,
                                        cos_table_t **resp_headers);


/*
 * @brief  cos copy large object using upload part copy
 * @param[in]   options             the cos request options
 * @param[in]   params              upload part copy parameters
 * @param[in]   headers             the headers for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_upload_part_copy(const cos_request_options_t *options,
                                   cos_upload_part_copy_params_t *params,
                                   cos_table_t *headers,
                                   cos_table_t **resp_headers);


/*
 * @brief  cos upload file using multipart upload
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   upload_id           the upload id to upload if has
 * @param[in]   filename            the filename containing object content
 * @param[in]   part_size           the part size for multipart upload
 * @param[in]   headers             the headers for request
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_upload_file(cos_request_options_t *options,
                              const cos_string_t *bucket,
                              const cos_string_t *object,
                              cos_string_t *upload_id,
                              cos_string_t *filename,
                              int64_t part_size,
                              cos_table_t *headers);

/*
 * @brief  cos upload object using part copy
 * @param[in]   options             the cos request options
 * @param[in]   copy_source         the cos copy source
 * @param[in]   dest_bucket         the cos dest bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   dest_object         the cos dest object name
 * @param[in]   part_size           the part size for multipart upload
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_upload_object_by_part_copy
(
        cos_request_options_t *options,
        const cos_string_t *copy_source,
        const cos_string_t *dest_bucket,
        const cos_string_t *dest_object,
        int64_t part_size
);

/*
 * @brief  cos download part to file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   download_file       the file with a specified part range to save the download content from cos object
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_download_part_to_file(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        cos_upload_file_t *download_file,
                                        cos_table_t **resp_headers);

/*
 * @brief  cos download part to file
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   download_file       the file with a specified part range to save the download content from cos object
 * @param[in]   progress_callback   the progress callback function
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_do_download_part_to_file(const cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *object,
                                           cos_upload_file_t *download_file,
                                           cos_progress_callback progress_callback,
                                           cos_table_t *headers,
                                           cos_table_t *params,
                                           cos_table_t **resp_headers);

/*
 * @brief  cos upload file with mulit-thread and resumable
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   filename            the filename containing object content
 * @param[in]   headers             the headers for request
 * @param[in]   params              the params for request
 * @param[in]   clt_params          the control params of upload
 * @param[in]   progress_callback   the progress callback function
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_resumable_upload_file(cos_request_options_t *options,
                                        cos_string_t *bucket,
                                        cos_string_t *object,
                                        cos_string_t *filepath,
                                        cos_table_t *headers,
                                        cos_table_t *params,
                                        cos_resumable_clt_params_t *clt_params,
                                        cos_progress_callback progress_callback,
                                        cos_table_t **resp_headers,
                                        cos_list_t *resp_body);

#if 0
/*
 * @brief  cos create live channel
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   config              the cos live channel configuration
 * @param[in]   publish_url_list    the publish url list
 * @param[in]   play_url_list       the play url list
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_create_live_channel(const cos_request_options_t *options,
                                      const cos_string_t *bucket,
                                      cos_live_channel_configuration_t *config,
                                      cos_list_t *publish_url_list,
                                      cos_list_t *play_url_list,
                                      cos_table_t **resp_headers);

/*
 * @brief  cos set live channel status
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   live_channel        the cos live channel name
 * @param[in]   live_channel_status the cos live channel status, enabled or disabled
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_put_live_channel_status(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *live_channel,
                                          const cos_string_t *live_channel_status,
                                          cos_table_t **resp_headers);

/*
 * @brief  cos get live channel information
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   live_channel        the cos live channel name
 * @param[out]  info                the cos live channel information
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_live_channel_info(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *live_channel,
                                        cos_live_channel_configuration_t *info,
                                        cos_table_t **resp_headers);

/*
 * @brief  cos get live channel stat
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   live_channel        the cos live channel name
 * @param[out]  stat                the cos live channel stat
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_live_channel_stat(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *live_channel,
                                        cos_live_channel_stat_t *stat,
                                        cos_table_t **resp_headers);

/*
 * @brief  delete cos live channel
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   live_channel        the cos live channel name
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_delete_live_channel(const cos_request_options_t *options,
                                      const cos_string_t *bucket,
                                      const cos_string_t *live_channel,
                                      cos_table_t **resp_headers);

/*
 * @brief  list cos live channels
 * @param[in]   options       the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   params        input params for list live channel request,
                              including prefix, marker, max_key
 * @param[out]  params        output params for list object response,
                              including truncated, next_marker, live channel list
 * @param[out]  resp_headers  cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_list_live_channel(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_list_live_channel_params_t *params,
                                    cos_table_t **resp_headers);

/*
 * @brief  cos get live record history of live channel
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   live_channel        the cos live channel name
 * @param[out]  live_record_list    the cos live records of live channel
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_get_live_channel_history(const cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *live_channel,
                                           cos_list_t *live_record_list,
                                           cos_table_t **resp_headers);

/*
 * @brief  generate vod play list for a period of time
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   live_channel        the cos live channel name
 * @param[in]   play_list_name      the cos live channel play list name
 * @param[in]   start_time          the start epoch time of play list, such as 1459922368
 * @param[in]   end_time            the end epoch time of play list, such as 1459922563
 * @param[out]  resp_headers        cos server response headers
 * @return  cos_status_t, code is 2xx success, other failure
 */
cos_status_t *cos_gen_vod_play_list(const cos_request_options_t *options,
                                     const cos_string_t *bucket,
                                     const cos_string_t *live_channel,
                                     const cos_string_t *play_list_name,
                                     const int64_t start_time,
                                     const int64_t end_time,
                                     cos_table_t **resp_headers);

/*
 * @brief  gen signed url for put rtmp stream
 * @param[in]   options             the cos request options
 * @param[in]   bucket        the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   live_channel        the cos live channel name
 * @param[in]   play_list_name      the cos live channel play list name
 * @param[in]   expires             the end expire time for signed url
 * @return  signed url, non-NULL success, NULL failure
 */
char *cos_gen_rtmp_signed_url(const cos_request_options_t *options,
                              const cos_string_t *bucket,
                              const cos_string_t *live_channel,
                              const cos_string_t *play_list_name,
                              const int64_t expires);
#endif

/*
 * @brief  generate a presigned cos url
 * @param[in]   options             the cos request options
 * @param[in]   bucket              the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
 * @param[in]   object              the cos object name
 * @param[in]   expire              the signature expire time, count as seconds
 * @param[in]   method              http request method, defined in enum http_method_e
 * @param[out]  presigned_url       the output of a presigned cos url
 * @return  defined in enum cos_error_code_e
 */
int cos_gen_presigned_url(const cos_request_options_t *options,
                          const cos_string_t *bucket,
                          const cos_string_t *object,
                          const int64_t expire,
                          http_method_e method,
                          cos_string_t *presigned_url);


COS_CPP_END

#endif
