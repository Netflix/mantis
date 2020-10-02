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

package io.mantisrx.master.api.akka.route.v1;

public class ParamName {
    public static String PROJECTION_FIELDS = "fields";
    public static String PROJECTION_TARGET = "fromObj";
    public static String SORT_BY = "sortBy";
    public static String SORT_ASCENDING = "ascending";
    public static String PAGINATION_LIMIT = "pageSize";
    public static String PAGINATION_OFFSET = "offset";

    public static String JOB_COMPACT = "compact";
    public static String JOB_FILTER_MATCH = "matching";
    public static String JOBCLUSTER_FILTER_MATCH = "matching";

    public static String REASON = "reason";
    public static String USER = "user";
    public static String SEND_HEARTBEAT = "sendHB";
    public static String ARCHIVED = "archived";
    public static String SERVER_FILTER_LIMIT = "limit";

}
