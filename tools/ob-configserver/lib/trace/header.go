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

package trace

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/oceanbase/configserver/logger"
)

func RandomTraceId() string {
	n := 8
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func ContextWithRandomTraceId() context.Context {
	return context.WithValue(context.Background(), logger.TraceIdKey{}, RandomTraceId())
}

func ContextWithTraceId(traceId string) context.Context {
	return context.WithValue(context.Background(), logger.TraceIdKey{}, traceId)
}
