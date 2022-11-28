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

#include "cos_define.h"

const char COS_CANNONICALIZED_HEADER_PREFIX[] = "x-cos-";
const char COS_CANNONICALIZED_HEADER_DATE[] = "x-cos-date";
const char COS_CANNONICALIZED_HEADER_ACL[] = "x-cos-acl";
const char COS_CANNONICALIZED_HEADER_COPY_SOURCE[] = "x-cos-copy-source";
const char COS_GRANT_READ[] = "x-cos-grant-read";
const char COS_GRANT_WRITE[] = "x-cos-grant-write";
const char COS_GRANT_FULL_CONTROL[] = "x-cos-grant-full-control";
const char COS_CONTENT_MD5[] = "Content-MD5";
const char COS_CONTENT_TYPE[] = "Content-Type";
const char COS_CONTENT_LENGTH[] = "Content-Length";
const char COS_DATE[] = "Date";
const char COS_AUTHORIZATION[] = "Authorization";
const char COS_ACCESSKEYID[] = "COSAccessKeyId";
const char COS_EXPECT[] = "Expect";
const char COS_TRANSFER_ENCODING[] = "Transfer-Encoding";
const char COS_HOST[] = "Host";
const char COS_RANGE[] = "Range";
const char COS_EXPIRES[] = "Expires";
const char COS_SIGNATURE[] = "Signature";
const char COS_ACL[] = "acl";
const char COS_ENCODING_TYPE[] = "encoding-type";
const char COS_PREFIX[] = "prefix";
const char COS_DELIMITER[] = "delimiter";
const char COS_MARKER[] = "marker";
const char COS_MAX_KEYS[] = "max-keys";
const char COS_RESTORE[] = "restore";
const char COS_UPLOADS[] = "uploads";
const char COS_UPLOAD_ID[] = "uploadId";
const char COS_MAX_PARTS[] = "max-parts";
const char COS_PART_NUMBER_MARKER[] = "part-number-marker";
const char COS_KEY_MARKER[] = "key-marker";
const char COS_UPLOAD_ID_MARKER[] = "upload-id-marker";
const char COS_MAX_UPLOADS[] = "max-uploads";
const char COS_PARTNUMBER[] = "partNumber";
const char COS_APPEND[] = "append";
const char COS_POSITION[] = "position";
const char COS_MULTIPART_CONTENT_TYPE[] = "application/x-www-form-urlencoded";
const char COS_COPY_SOURCE[] = "x-cos-copy-source";
const char COS_COPY_SOURCE_RANGE[] = "x-cos-copy-source-range";
const char COS_SECURITY_TOKEN[] = "security-token";
const char COS_STS_SECURITY_TOKEN[] = "x-cos-security-token";
const char COS_REPLACE_OBJECT_META[] = "x-cos-replace-object-meta";
const char COS_OBJECT_TYPE[] = "x-cos-object-type";
const char COS_NEXT_APPEND_POSITION[] = "x-cos-next-append-position";
const char COS_HASH_CRC64_ECMA[] = "x-cos-hash-crc64ecma";
const char COS_CONTENT_SHA1[] = "x-cos-content-sha1";
const char COS_CALLBACK[] = "x-cos-callback";
const char COS_CALLBACK_VAR[] = "x-cos-callback-var";
const char COS_PROCESS[] = "x-cos-process";
const char COS_LIFECYCLE[] = "lifecycle";
const char COS_CORS[] = "cors";
const char COS_REPLICATION[] = "replication";
const char COS_VERSIONING[] = "versioning";
const char COS_WEBSITE[] = "website";
const char COS_DOMAIN[] = "domain";
const char COS_DELETE[] = "delete";
const char COS_LOGGING[] = "logging";
const char COS_INVENTORY[] = "inventory";
const char COS_TAGGING[] = "tagging";
const char COS_YES[] = "yes";
const char COS_OBJECT_TYPE_NORMAL[] = "normal";
const char COS_OBJECT_TYPE_APPENDABLE[] = "appendable";
const char COS_LIVE_CHANNEL[] = "live";
const char COS_LIVE_CHANNEL_STATUS[] = "status";
const char COS_COMP[] = "comp";
const char COS_LIVE_CHANNEL_STAT[] = "stat";
const char COS_LIVE_CHANNEL_HISTORY[] = "history";
const char COS_LIVE_CHANNEL_VOD[] = "vod";
const char COS_LIVE_CHANNEL_START_TIME[] = "startTime";
const char COS_LIVE_CHANNEL_END_TIME[] = "endTime";
const char COS_PLAY_LIST_NAME[] = "playlistName";
const char LIVE_CHANNEL_STATUS_DISABLED[] = "disabled";
const char LIVE_CHANNEL_STATUS_ENABLED[] = "enabled";
const char LIVE_CHANNEL_STATUS_IDLE[] = "idle";
const char LIVE_CHANNEL_STATUS_LIVE[] = "live";
const char LIVE_CHANNEL_DEFAULT_TYPE[] = "HLS";
const char LIVE_CHANNEL_DEFAULT_PLAYLIST[] = "playlist.m3u8";
const int  LIVE_CHANNEL_DEFAULT_FRAG_DURATION = 5;
const int  LIVE_CHANNEL_DEFAULT_FRAG_COUNT = 3;
const int COS_MAX_PART_NUM = 10000;
const int COS_PER_RET_NUM = 1000;
const int MAX_SUFFIX_LEN = 1024;
const char COS_INTELLIGENTTIERING[] = "intelligenttiering";
