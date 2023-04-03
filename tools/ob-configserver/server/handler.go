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

package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/oceanbase/configserver/lib/codec"
	"github.com/oceanbase/configserver/lib/net"
	"github.com/oceanbase/configserver/lib/trace"
)

var invalidActionOnce sync.Once
var invalidActionFunc func(*gin.Context)

func getInvalidActionFunc() func(*gin.Context) {
	invalidActionOnce.Do(func() {
		invalidActionFunc = handlerFunctionWrapper(invalidAction)
	})
	return invalidActionFunc
}

func getServerIdentity() string {
	ip, _ := net.GetLocalIpAddress()
	return ip
}

func handlerFunctionWrapper(f func(context.Context, *gin.Context) *ApiResponse) func(*gin.Context) {
	fn := func(c *gin.Context) {
		tStart := time.Now()
		traceId := trace.RandomTraceId()
		ctxlog := trace.ContextWithTraceId(traceId)
		log.WithContext(ctxlog).Infof("handle request: %s %s", c.Request.Method, c.Request.RequestURI)
		response := f(ctxlog, c)
		cost := time.Now().Sub(tStart).Milliseconds()
		response.TraceId = traceId
		response.Cost = cost
		response.Server = getServerIdentity()
		responseJson, err := codec.MarshalToJsonString(response)
		if err != nil {
			log.WithContext(ctxlog).Errorf("response: %s", "response serialization error")
			c.JSON(http.StatusInternalServerError, NewErrorResponse(errors.Wrap(err, "serialize response")))
		} else {
			log.WithContext(ctxlog).Infof("response: %s", responseJson)
			c.String(response.Code, string(responseJson))
		}
	}
	return fn
}

func invalidAction(ctxlog context.Context, c *gin.Context) *ApiResponse {
	log.WithContext(ctxlog).Error("invalid action")
	return NewIllegalArgumentResponse(errors.New("invalid action"))
}

func getHandler() gin.HandlerFunc {
	fn := func(c *gin.Context) {
		action := c.Query("Action")
		switch action {
		case "ObRootServiceInfo":
			getObRootServiceGetFunc()(c)

		case "GetObProxyConfig":
			getObProxyConfigFunc()(c)

		case "GetObRootServiceInfoUrlTemplate":
			getObProxyConfigWithTemplateFunc()(c)

		case "ObIDCRegionInfo":
			getObIdcRegionInfoFunc()(c)

		default:
			getInvalidActionFunc()(c)
		}
	}
	return gin.HandlerFunc(fn)
}

func postHandler() gin.HandlerFunc {

	fn := func(c *gin.Context) {
		action, _ := c.GetQuery("Action")
		switch action {
		case "ObRootServiceInfo":
			getObRootServicePostFunc()(c)

		case "GetObProxyConfig":
			getObProxyConfigFunc()(c)

		case "GetObRootServiceInfoUrlTemplate":
			getObProxyConfigWithTemplateFunc()(c)
		default:
			getInvalidActionFunc()(c)
		}
	}

	return gin.HandlerFunc(fn)
}

func deleteHandler() gin.HandlerFunc {
	fn := func(c *gin.Context) {
		action, _ := c.GetQuery("Action")
		switch action {
		case "ObRootServiceInfo":
			getObRootServiceDeleteFunc()(c)
		default:
			getInvalidActionFunc()(c)
		}
	}
	return gin.HandlerFunc(fn)
}
