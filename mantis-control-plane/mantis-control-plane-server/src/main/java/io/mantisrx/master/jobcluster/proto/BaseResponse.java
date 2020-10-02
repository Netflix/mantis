/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.master.jobcluster.proto;

import com.netflix.spectator.impl.Preconditions;


public class BaseResponse {
    public enum ResponseCode {
        SUCCESS(200),                  //200
        SUCCESS_CREATED(201),          //201
        CLIENT_ERROR(400),             //400
        CLIENT_ERROR_NOT_FOUND(404),   //404
        OPERATION_NOT_ALLOWED(405),    //405
        CLIENT_ERROR_CONFLICT(409),    //409
        SERVER_ERROR(500);             //500

        private final int value;

        ResponseCode(int val) {
            this.value = val;
        }

        public int getValue() {
            return this.value;
        }
    }

    public final long requestId;
    public final ResponseCode responseCode;
    public final String message;

    public BaseResponse(
            final long requestId,
            final ResponseCode responseCode,
            final String message) {
        Preconditions.checkNotNull(responseCode, "Response code cannot be null");
//        Preconditions.checkArg(message != null, "message cannot be null");

        this.requestId = requestId;
        this.responseCode = responseCode;
        this.message = message;

    }

}
