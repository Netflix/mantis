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

package io.reactivx.common.compression;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.compression.CompressionUtils;
import org.junit.Test;


public class CompressionUtilsTest {

    String e1 = "{\"geo.domain\":\"telefonica.de\",\"country\":\"DE\",\"esn\":\"NFPS3-001-8A5YN7NN6JPRA4D2JNEVR52FXR\",\"stack\":\"prod\",\"code\":\"unknown\",\"device.typeId\":\"129\",\"response.header.X-Netflix.api-script-endpoint\":\"/nrdjs/2.4.8\",\"mantis.meta.sourceName\":\"APIAllRequestSource\",\"type\":\"EVENT\",\"msl.request.translated.header.x-netflix-account-id\":\"864871039131224471\",\"duration\":16,\"path\":\"/msl/nrdjs/2.4.8\",\"customer.signupCountry\":\"null\",\"request.header.x-netflix.request.toplevel.uuid\":\"43ff9d19-3d7e-473e-b09b-6203c5ae8e66-34831855\",\"matched-clients\":[\"MantisRequestEvents_APIRequestSource-44_1491597861375_248288163\"],\"playback_action\":\"APIPlaybackKeepAliveAction\",\"response.header.X-Netflix.api-script-execution-time\":\"7\",\"instance.id\":\"i-07880ca5ef54208f4\",\"geo.region\":\"HH\",\"geo.city\":\"HAMBURG\",\"currentTime\":1492708185876,\"mantis.meta.timestamp\":1492708186348,\"asg\":\"api-prod-v417\",\"request.header.user-agent\":\"Gibbon/2016.1.2/2016.1.2: Netflix/2016.1.2 (DEVTYPE=NFPS3-001-; CERTVER=0)\",\"request.header.x-forwarded-for\":\"77.187.195.130\",\"response.header.X-Netflix.api-script-scope\":\"java-1.7.0+groovy-2.3.6:com.netflix.api.endpoint.BaseEndpoint:API:prod:eu-west-1:/nrdjs/2.4.8:1\",\"response.header.X-Netflix.api-script-primer-dependencies\":\"\",\"customer_id\":\"3\",\"asn\":\"unknown\",\"device\":\"129\",\"operation\":\"onComplete\",\"device.operationalName\":\"ps3\",\"status\":200}";

    String e2 = "{\"geo.domain\":\"has.dk\",\"country\":\"DK\",\"esn\":\"NFPS3-001-2JEL5KMPFH9XRMKGYU900LEF1M\",\"stack\":\"prod\",\"code\":\"unknown\",\"device.typeId\":\"129\",\"response.header.X-Netflix.api-script-endpoint\":\"/nrdjs/2.4.8\",\"mantis.meta.sourceName\":\"APIAllRequestSource\",\"type\":\"EVENT\",\"msl.request.translated.header.x-netflix-account-id\":\"864865529565852040\",\"duration\":11,\"path\":\"/msl/nrdjs/2.4.8\",\"customer.signupCountry\":\"null\",\"request.header.x-netflix.request.toplevel.uuid\":\"21b00713-6f08-461e-b0b7-319df50afa23-10055871\",\"matched-clients\":[\"MantisRequestEvents_APIRequestSource-44_1491597860848_248288163\"],\"playback_action\":\"APIPlaybackReportLogAction\",\"response.header.X-Netflix.api-script-execution-time\":\"4\",\"instance.id\":\"i-06bd04675b9e02675\",\"geo.city\":\"COPENHAGEN\",\"currentTime\":1492708185604,\"mantis.meta.timestamp\":1492708186348,\"asg\":\"api-prod-v417\",\"request.header.user-agent\":\"Gibbon/2016.1.2/2016.1.2: Netflix/2016.1.2 (DEVTYPE=NFPS3-001-; CERTVER=0)\",\"request.header.x-forwarded-for\":\"185.126.109.138\",\"response.header.X-Netflix.api-script-scope\":\"java-1.7.0+groovy-2.3.6:com.netflix.api.endpoint.BaseEndpoint:API:prod:eu-west-1:/nrdjs/2.4.8:1\",\"response.header.X-Netflix.api-script-primer-dependencies\":\"\",\"customer_id\":\"4\",\"asn\":\"unknown\",\"device\":\"129\",\"operation\":\"onComplete\",\"device.operationalName\":\"ps3\",\"status\":200}";

    String e3 = "{\"nf.country\":\"FI\",\"cluster\":\"api-prod\",\"request.header.x-gibbon-cache-control\":\"no-cache\",\"stack\":\"prod\",\"request.header.x-netflix-from-zuul\":\"true\",\"request.header.x-netflix.clientid\":\"TkZQUzQtMDAxLTFEVzVZN0VWTTdYSjUxVjFXN0hFQ0U1SEUw\",\"msl.request.translated.method\":\"POST\",\"type\":\"EVENT\",\"msl.request.translated.header.X-Netflix.esn\":\"NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0\",\"path\":\"/msl/nrdjs/2.4.8\",\"request.header.x-netflix.request.toplevel.uuid\":\"e8dceb91-9796-4ab6-97ba-a7b61bc08a03-18963523\",\"msl.request.translated.header.x-netflix-user-id\":\"864875369580174523\",\"request.header.x-netflix.request.attempt\":\"1\",\"response.header.X-Robots-Tag\":\"noindex, nofollow\",\"request.header.x-netflix.client.cluster.name\":\"apiproxy-prod\",\"playback_action\":\"APIAcquireDrmLicenseAction\",\"response.header.X-Netflix.api-script-status\":\"unwarmed\",\"method\":\"POST\",\"msl.request.translated.hmac\":\"3:72b0ed955cb8e61c:NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0:864875369580170768:864875369580174523:null:false:AQEBYAABASB1I1uCOjyr6XjPwn3jZUZeE3vLYGv+dI+AfLuOlmBpf7bcNC4=\",\"geo.zip\":null,\"request.header.x-devicemodel\":\"PS4??? Pollux\",\"msl.request.translated.customer_id\":\"5\",\"currentTime\":1492708185537,\"response.header.X-Netflix.dependency-command.executions\":\"CryptexMacVerifyViaSemaphore[SUCCESS][0ms]x6, CryptexDecryptViaSemaphore[SUCCESS][0ms]x4, CryptexMacGenerateViaSemaphore[SUCCESS][0ms]x2, WIDEVINE_STANDARD_LICENSE_COMMAND[SUCCESS][16ms], PDS_RESERVATION_EVENT[SUCCESS][6ms], CryptexEncryptViaSemaphore[SUCCESS][0ms]\",\"request.header.user-agent\":\"Gibbon/2017.1.2/2017.1.2: Netflix/2017.1.2 (DEVTYPE=NFPS4-001-; CERTVER=0)\",\"request.header.x-forwarded-for\":\"85.23.222.179\",\"request.header.x-allowcompression\":\"true\",\"request.header.x-netflix.request.sub.context.geodata\":\"1|com.netflix.geoclient.GeoDataContextSerializer|1|continent=EU&ipaddress=85.23.222.179&city=RAUMA&timezone=GMT%2B2&long=21.50&country_code=FI&bw=5000&domain=dnainternet.fi&asnum=16086&dma=-2&company=DNA_Oy&throughput=vhigh&lat=61.13\",\"device\":\"953\",\"device.operationalName\":\"ps4\",\"status\":200,\"exceptionType\":\"unknown\",\"deviceTypeId\":\"953\",\"response.header.X-Netflix.api-script-endpoint\":\"/nrdjs/2.4.8\",\"request.header.content-type\":\"application/x-www-form-urlencoded\",\"msl.request.translated.esn\":\"NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0\",\"request.header.x-forwarded-proto-version\":\"HTTP/1.1\",\"request.header.x-netflix.environment\":\"prod\",\"operationalName\":\"ps4\",\"matched-clients\":[\"MantisRequestEvents_APIRequestSource-44_1491597861144_755049216\",\"MantisRequestEvents_APIRequestSource-44_1491597861144_248288163\",\"MantisRequestEvents_APIRequestSource-44_1491597861144_1683101079\",\"MantisRequestEvents_APIRequestSource-44_1491597861144_1186455283\",\"MantisRequestEvents_APIRequestSource-44_1491597861144_1417360158\"],\"instance.ec2InstanceId\":\"i-06123cbf51e0be4e1\",\"msl.request.translated.query\":\"esn=NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0&logs=false&output=json&sdk=2017.1.2&platform=2017.1.2&application=Pollux-531&uiversion=UI-release-20170410_4580_2-gibbon-sapphire-darwinql&protocol=2.4.8\",\"request.header.x-netflix.client.asg.name\":\"apiproxy-prod-v183\",\"geo.city\":\"RAUMA\",\"msl.request.translated.path\":\"/nrdjs/2.4.8\",\"request.header.x-netflix.client.requeststarttime\":\"1492708185477\",\"request.header.accept\":\"*/*\",\"asg\":\"api-prod-v417\",\"response.header.X-Netflix.api-script-scope\":\"java-1.7.0+groovy-2.3.6:com.netflix.api.endpoint.BaseEndpoint:API:prod:eu-west-1:/nrdjs/2.4.8:1\",\"com.netflix.api.service.playback.ThrowingMslEnforcer-mslVerification-enabled\":true,\"request.header.x-netflix.client.appid\":\"apiproxy\",\"customer_id\":\"6\",\"request.header.x-forwarded-port\":\"80\",\"asn\":\"unknown\",\"operation\":\"onComplete\",\"request.header.x-forwarded-proto\":\"http\",\"response.header.X-Netflix.api-script-threads\":\"Total[0] MaxConcurrent[0]\",\"geo.domain\":\"dnainternet.fi\",\"esn\":\"NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0\",\"country\":\"FI\",\"request.header.x-netflix.client-host\":\"api-global.netflix.com\",\"mantis.meta.sourceName\":\"APIAllRequestSource\",\"device_type\":\"953\",\"playbackLicenseClient-licenseResponsePayload\":\"{\\\"SUCCESS\\\":true,\\\"DURATION_SECS\\\":43200,\\\"EXPIRATION\\\":1492751385,\\\"RESPONSE_BYTES_B64\\\":\\\"CAISzgIKmQIKIEI2MDgwQkY5Q0NCQUY0M0ExQzAwMDAwMDAwMDAwMDAwEpcBeyJ2ZXJzaW9uIjoiMS4wIiwiZXNuIjoiTkZQUzQtMDAxLTFEVzVZN0VWTTdYSjUxVjFXN0hFQ0U1SEUwIiwic2FsdCI6IjY2MTk2MzUxMDI2Nzg2LTI5MTQ1NTQ1MjIyMjQ0MDQ2IiwiaXNzdWVUaW1lIjoxNDkyNzA4MTY3MDAwLCJtb3ZpZUlkIjoiODAwODQ2NzIifSABKAEyVzY2MTk2MzUxMDYzNzgzNjQ5MTg2NTY5Mzk0NTc1MDE1MT03MjU4OlRrWlFVelF0TURBeExURkVWelZaTjBWV1RUZFlTalV4VmpGWE4waEZRMFUxU0VVdxISCAEQABgAKMDRAjDA0QJYAGABGhYgA0ISChBrYzA5AACowIpbyOgAACAIINfW48cFGiBJu4j7NU0c0rbKa1emhBcRruNZxkKqMcICNdaQu4zFdg==\\\",\\\"ID\\\":\\\"licenseGuid\\\",\\\"CDMID\\\":\\\"7258:TkZQUzQtMDAxLTFEVzVZN0VWTTdYSjUxVjFXN0hFQ0U1SEUw\\\",\\\"USECDMIDASDEVICEID\\\":false,\\\"CDMVERSION\\\":\\\"Pollux-531\\\"}\",\"translated\":\"unknown\",\"request.header.x-forwarded-host\":\"api-global.netflix.com\",\"request.header.x-netflix-loadbalancer-host\":\"api-global.netflix.com\",\"request.header.accept-encoding\":\"deflate, gzip\",\"primerScope\":\"{\\\"runtime\\\":\\\"java-1.7.0+groovy-2.3.6\\\",\\\"service\\\":\\\"com.netflix.api.endpoint.BaseEndpoint\\\",\\\"application\\\":\\\"API\\\",\\\"stack\\\":\\\"prod\\\",\\\"region\\\":\\\"eu-west-1\\\",\\\"name\\\":\\\"/nrdjs/2.4.8\\\",\\\"version\\\":\\\"1\\\"}\",\"request.header.referer\":\"https://secure.netflix.com/us/tvui/ql/patch/20170410_4580/2/release/darwinql.js?dh=720&q=&dw=1280&dar=16_9&reg=true&bootloader_trace=apiusernotnull__false\",\"request.header.x-netflix.request.sub.context.rcsubcust\":\"1|com.netflix.server.context.StringSerializer|1|1,0,2,864875369580174523,864875369580170768\",\"host\":\"api-global.netflix.com\",\"instance.netflix.stack\":\"prod\",\"playbackLicenseClient-entity\":\"{\\\"LicenseRequest\\\":{\\\"LICENSE_PLAYBACK_CONTEXT\\\":{\\\"VIDEO_ENCODING_FORMAT\\\":\\\"CE3\\\",\\\"DRM_HEADER\\\":{\\\"CHECKSUM\\\":\\\"\\\",\\\"DRM_SYSTEM_TYPE\\\":\\\"EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED\\\",\\\"MAX_WIDTH_PIXELS\\\":1920,\\\"MAX_HEIGHT_PIXELS\\\":1080,\\\"GLOBAL_KEY_ID\\\":\\\"AAAAAAP2qcsAAAAAAAAAAA==\\\",\\\"KEY_ID\\\":\\\"AAAAAAP2qcsAAAAAAAAAAA==\\\",\\\"HEADER_BYTES\\\":\\\"CAESEAAAAAAD9qnLAAAAAAAAAAA=\\\"},\\\"MOVIE_LENGTH\\\":2781,\\\"MAX_HEIGHT\\\":1080,\\\"PRK_DRM_HEADERS\\\":[{\\\"CHECKSUM\\\":\\\"\\\",\\\"DRM_SYSTEM_TYPE\\\":\\\"EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED\\\",\\\"MAX_WIDTH_PIXELS\\\":1920,\\\"MAX_HEIGHT_PIXELS\\\":1080,\\\"GLOBAL_KEY_ID\\\":\\\"AAAAAAP2qcsAAAAAAAAAAA==\\\",\\\"KEY_ID\\\":\\\"AAAAAAP2qcsAAAAAAAAAAA==\\\",\\\"HEADER_BYTES\\\":\\\"CAESEAAAAAAD9qnLAAAAAAAAAAA=\\\"}],\\\"OFFLINE_DOWNLOAD_DATA\\\":{\\\"SEASON_ID\\\":80067702,\\\"OFFLINE_DOWNLOAD_END_DATE\\\":-1},\\\"VIEWABLE_ID\\\":80084672,\\\"CUP_TOKENS\\\":[\\\"10002\\\"],\\\"PACKAGE_ID\\\":1196770,\\\"PER_RESOLUTION_KEY_REQUEST\\\":false},\\\"DRM_CONTEXT_ID\\\":\\\"E1-BQFRAAELEEMtON_0ePEkkiWAZWRl7gFwPUcZRVrBIdyjxedCXHqxFIU2Zvbk8dt06FlBZQ1fOwOdNtRLMK-AgXFYKucnr0yMURa2IJzuTprIsJTfQ461j1_NEOuEwdVNR0Ymc6eisyK7k0aQycxwj6vl2s_yFQVuD8xIUjZjf1LOJHbtUkYLSg..;EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED;STANDARD;1492708185506\\\",\\\"XID\\\":149270817943804727,\\\"INTEGRATION_CONTEXT\\\":{\\\"ESN\\\":\\\"NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0\\\",\\\"ACCOUNT_ID\\\":864875369580170768,\\\"CUSTOMER_ID\\\":7,\\\"SERVER_TIME_UTC\\\":1492708185477,\\\"GEO_PROPERTIES\\\":{\\\"continent\\\":\\\"EU\\\",\\\"ipaddress\\\":\\\"85.23.222.179\\\",\\\"city\\\":\\\"RAUMA\\\",\\\"timezone\\\":\\\"GMT+2\\\",\\\"long\\\":\\\"21.50\\\",\\\"country_code\\\":\\\"FI\\\",\\\"bw\\\":\\\"5000\\\",\\\"domain\\\":\\\"dnainternet.fi\\\",\\\"asnum\\\":\\\"16086\\\",\\\"dma\\\":\\\"-2\\\",\\\"company\\\":\\\"DNA_Oy\\\",\\\"throughput\\\":\\\"vhigh\\\",\\\"lat\\\":\\\"61.13\\\"}},\\\"DRM_LICENSE_TYPE\\\":\\\"STANDARD\\\",\\\"CLIENT_DRM_TIME\\\":1492708183000,\\\"DRM_SYSTEM_TYPE\\\":\\\"EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED\\\",\\\"CLIENT_SYSTEM_TIME\\\":0,\\\"PLAYBACK_CONTEXT_ID\\\":\\\"E1-BQFRAAELEEMtON_0ePEkkiWAZWRl7gFwPUcZRVrBIdyjxedCXHqxFIU2Zvbk8dt06FlBZQ1fOwOdNtRLMK-AgXFYKucnr0yMURa2IJzuTprIsJTfQ461j1_NEOuEwdVNR0Ymc6eisyK7k0aQycxwj6vl2s_yFQVuD8xIUjZjf1LOJHbtUkYLSg..\\\",\\\"VIEWABLE_ID\\\":80084672,\\\"CHALLENGE_B64\\\":\\\"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\\\",\\\"CLIENT_TIME\\\":1492708183000}}\",\"instance.id\":\"i-06123cbf51e0be4e1\",\"query\":null,\"device.category\":\"PS4\",\"nrdjs.moduleLogs\":[],\"request.header.x-netflix.request.uuid\":\"e8dceb91-9796-4ab6-97ba-a7b61bc08a03-18963523\",\"playbackLicenseClient-urlCalled\":\"http://100.83.163.242:7001/2.0/licenseService/widevine/standard\",\"mantis.meta.timestamp\":1492708186348,\"msl.request.translated.header.X-Netflix-msl-hmac\":\"3:72b0ed955cb8e61c:NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0:864875369580170768:864875369580174523:null:false:AQEBYAABASB1I1uCOjyr6XjPwn3jZUZeE3vLYGv+dI+AfLuOlmBpf7bcNC4=\",\"response.content-length\":6951,\"request.header.x-netflix.request.expiry.timeout\":\"14623\",\"response.header.X-Netflix.api-script-primer-dependencies\":\"\",\"playbackLicenseClient-uriPath\":\"/2.0/licenseService/widevine/standard\",\"response.header.X-Netflix.api-script-execution-count\":\"2953128\",\"code\":\"unknown\",\"request.header.language\":\"fi-FI,fi,en-FI,en-GB,en\",\"msl.response.translated.message.status\":\"200\",\"device.typeId\":\"953\",\"response.header.set-cookie\":\"memclid=TkZQUzQtMDAxLTFEVzVZN0VWTTdYSjUxVjFXN0hFQ0U1SEUw; Domain=.netflix.com; Max-Age=31536000; Path=/\",\"request.header.x-netflix.client.az\":\"eu-west-1c\",\"device.id\":\"NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0\",\"request.header.transfer-encoding\":\"chunked\",\"response.header.X-Netflix.msl-decrypt-time\":\"3\",\"msl.request.translated.header.x-netflix-account-id\":\"864875369580170768\",\"duration\":59,\"random\":1538606962,\"response.header.X-Netflix.msl-encrypt-time\":\"2\",\"customer.signupCountry\":\"null\",\"request.header.x-netflix.client-proto\":\"http\",\"request.header.x-client-request-id\":\"149270818491709038\",\"request.header.connection\":\"keep-alive\",\"request.header.x-netflix.edge.server.timestamp\":\"1492708185477\",\"response.header.X-Netflix.api-script-execution-time\":\"44\",\"request.header.x-netflix.request.sub.context.names\":\"X-Netflix.request.sub.context.RCSubCust,X-Netflix.request.sub.context.GeoData\",\"geo.region\":null,\"request.header.host\":\"api-global.netflix.com\",\"request.header.cookie\":\"memclid=TkZQUzQtMDAxLTFEVzVZN0VWTTdYSjUxVjFXN0hFQ0U1SEUw; nfvdid=BQFmAAEBEOhEWcaW4%2FzDfFKUwnOLqWpQ4wTX9zT7E7c4JhA5f7U9aIY6NxxBtSFvyERt1yMltsadsjI9aSxy6LOkuckObxUO2MixoAJP2CHv32RLzyfyc9uHE%2BwHoDgQl6H4xf0cabo%3D; NetflixId=ct%3DBQAOAAEBEIa0ejqGKfUPlJmoZHYX2KyB8ENtgm8vuaYxnl8Z5H_WCDia_ilJT3b2IuZPmbnedpMnsodJctPju1H_Z7WBwwlvts_3POPNBuZLkR1mP7ai-u5RMRGa5rFM9-WRatfrNcq6_M15hLbefac8CJTFOTPDFosVc5ghxeMFZm5R04ujq-WIbfelHt0cTF9cSI3dy7cE09CtoULx9BydhfWEa-fhnOoMGQsZpHGJCpoZnGIESUufaEn9b3oeYEZW0TNQiDn0jz7vnbJq3hMpvUBSqcbQOmn0hZkoNIkK_i-IyK_nH_jaAe69It0BFTTVf9WYIUb3Bme-ngLyfGhFsPdU37g8WhVIZzkQ-wk6JhXB9lxWJpZ1GDa45MpJxdswRKsTeCE3G5hTkwMZDyjW1NdIIL6fvm74lz-sHaGeN95aJVgcHlJB4UtR5IPWyNN_zbW_jwkEy6INVFCD1Y43OxlpsdglM3CJQ1zqPPdGBYxy5qNtMDGIN7gauDZA7_GgXudFXoc3L88ur7yz0kJtwiabEpb9NiI1QU-PSwanGcvoalYGNEne_7Jn3ZTionmaoYHj0OlO6QgA3i0yHxG0wqkSGQv4eZGnJOcWUVAS4NdjHP4VmaZh-iNAnIAbSi2OnuxUB-GQLVN0LWF1wHKTmPWBoR1i-aNwWyQH-QAsKVxsfnpQWmo.%26bt%3Ddbl%26ch%3DAQEAEAABABTXxU-rr-Fr6e9PjAbryGiLH_SCTSla7os.%26v%3D2%26mac%3DAQEAEAABABR6FD0EdS7Fl6JeCx_Wg2OSrpEmOI340nw.\",\"msl.wrappedResponse.contentType\":\"application/x-json-hack\",\"request.header.x-netflix.client.attempt\":\"1\",\"isInternalIP\":false,\"request.header.x-netflix.client.instid\":\"i-0855a90646833eccd\",\"request_id\":\"e8dceb91-9796-4ab6-97ba-a7b61bc08a03-18963523\",\"request.header.x-netflix.request.context\":\"reqId=e8dceb91-9796-4ab6-97ba-a7b61bc08a03-18963523&appName=APIPROXY&appVersion=unknown&devType=953&devId=NFPS4-001-1DW5Y7EVM7XJ51V1W7HECE5HE0&isoCountry=FI&deviceIp=85.23.222.179&gmtOffset=7200000&tz=GMT%2B2&deviceIpAddrFamily=IPv4&X-clientID=TkZQUzQtMDAxLTFEVzVZN0VWTTdYSjUxVjFXN0hFQ0U1SEUw&X-rq-nfvdid=id%3Dcbe8df88-f44a-4044-ab16-e3f19a0a1a40%26ts%3D1492439774395%26ae%3D0\"}";

    String e4 = "{\"geo.domain\":\"btireland.net\",\"country\":\"IE\",\"esn\":\"LGTV20169=91002701619\",\"stack\":\"prod\",\"code\":\"unknown\",\"device.typeId\":\"1406\",\"response.header.X-Netflix.api-script-endpoint\":\"/nrdjs/2.4.8\",\"mantis.meta.sourceName\":\"APIAllRequestSource\",\"type\":\"EVENT\",\"msl.request.translated.header.x-netflix-account-id\":\"1315580382\",\"duration\":20,\"path\":\"/msl/nrdjs/2.4.8\",\"customer.signupCountry\":\"null\",\"request.header.x-netflix.request.toplevel.uuid\":\"7884078e-211e-40ce-ba5a-05f27679829f-12215197\",\"matched-clients\":[\"MantisRequestEvents_APIRequestSource-44_1491597861375_248288163\"],\"playback_action\":\"APIPlaybackKeepAliveAction\",\"response.header.X-Netflix.api-script-execution-time\":\"8\",\"instance.id\":\"i-07880ca5ef54208f4\",\"geo.city\":\"DUBLIN\",\"currentTime\":1492708185931,\"mantis.meta.timestamp\":1492708186348,\"asg\":\"api-prod-v417\",\"request.header.user-agent\":\"Gibbon/2015.1.1/2015.1.1: Netflix/2015.1.1 (DEVTYPE=LGTV20169=; CERTVER=0)\",\"request.header.x-forwarded-for\":\"78.16.247.191\",\"response.header.X-Netflix.api-script-scope\":\"java-1.7.0+groovy-2.3.6:com.netflix.api.endpoint.BaseEndpoint:API:prod:eu-west-1:/nrdjs/2.4.8:1\",\"response.header.X-Netflix.api-script-primer-dependencies\":\"\",\"customer_id\":\"8\",\"asn\":\"unknown\",\"device\":\"1406\",\"operation\":\"onComplete\",\"device.operationalName\":\"ce\",\"status\":200}";

    String e5 = "{\"geo.domain\":\"telepac.pt\",\"country\":\"PT\",\"esn\":\"GOOGEAR0010000000000000016756020\",\"stack\":\"prod\",\"code\":\"unknown\",\"device.typeId\":\"1416\",\"response.header.X-Netflix.api-script-endpoint\":\"/cbp/cadmium-14\",\"mantis.meta.sourceName\":\"APIAllRequestSource\",\"type\":\"EVENT\",\"msl.request.translated.header.x-netflix-account-id\":\"792822967140419768\",\"duration\":16,\"path\":\"/msl/GOOGEAR001/cadmium/logblob\",\"customer.signupCountry\":\"null\",\"request.header.x-netflix.request.toplevel.uuid\":\"9f0b527b-0132-487f-8026-d6b264b561ba-1107595\",\"matched-clients\":[\"MantisRequestEvents_APIRequestSource-44_1491597860848_248288163\"],\"playback_action\":\"APIPlaybackReportLogAction\",\"response.header.X-Netflix.api-script-execution-time\":\"5\",\"instance.id\":\"i-0be9702801874a764\",\"geo.city\":\"LISBON\",\"currentTime\":1492708185879,\"mantis.meta.timestamp\":1492708186348,\"asg\":\"api-prod-v417\",\"request.header.user-agent\":\"Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.84 Safari/537.36 CrKey/1.22.79313\",\"request.header.x-forwarded-for\":\"82.154.44.113\",\"response.header.X-Netflix.api-script-scope\":\"java-1.7.0+groovy-2.3.6:com.netflix.api.endpoint.BaseEndpoint:API:prod:eu-west-1:/cbp/cadmium-14:1.0\",\"response.header.X-Netflix.api-script-primer-dependencies\":\":*::::cadmium-manifest-common:1.2.6 :*::::cadmium-prefetch-shared-secret-common:1.0.2 \",\"customer_id\":\"1\",\"asn\":\"unknown\",\"device\":\"1416\",\"operation\":\"onComplete\",\"device.operationalName\":\"streaming_stick\",\"status\":200}";

    String e6 = "{\"geo.domain\":\"rdsnet.ro\",\"country\":\"RO\",\"esn\":\"SSTV-JZL1-0000000000000008803009\",\"stack\":\"prod\",\"code\":\"unknown\",\"device.typeId\":\"1528\",\"response.header.X-Netflix.api-script-endpoint\":\"/nrdjs/2.4.8\",\"mantis.meta.sourceName\":\"APIAllRequestSource\",\"type\":\"EVENT\",\"duration\":10,\"path\":\"/msl/nrdjs/2.4.8\",\"customer.signupCountry\":\"null\",\"request.header.x-netflix.request.toplevel.uuid\":\"0bf3255e-ee03-4cfa-bd1a-eae9103da795-34335159\",\"matched-clients\":[\"MantisRequestEvents_APIRequestSource-44_1491597861375_248288163\"],\"playback_action\":\"APIPlaybackReportLogAction\",\"response.header.X-Netflix.api-script-execution-time\":\"6\",\"instance.id\":\"i-07880ca5ef54208f4\",\"geo.city\":\"TARGOVISTE\",\"currentTime\":1492708185832,\"mantis.meta.timestamp\":1492708186347,\"asg\":\"api-prod-v417\",\"request.header.user-agent\":\"Gibbon/2015.1.1/2015.1.1: Netflix/2015.1.1 (DEVTYPE=SSTV-JZL1-; CERTVER=1)\",\"request.header.x-forwarded-for\":\"79.114.176.105\",\"response.header.X-Netflix.api-script-scope\":\"java-1.7.0+groovy-2.3.6:com.netflix.api.endpoint.BaseEndpoint:API:prod:eu-west-1:/nrdjs/2.4.8:1\",\"response.header.X-Netflix.api-script-primer-dependencies\":\"\",\"customer_id\":\"2\",\"asn\":\"unknown\",\"device\":\"1528\",\"operation\":\"onComplete\",\"device.operationalName\":\"ce\",\"status\":200}";

    String data[] = {e1, e2, e3, e4, e5, e6};


    //	@Test
    //	public void basicTest() {
    //
    //		String encodedString = CompressionUtils.compressAndBase64Encode(Arrays.asList(data));
    //
    //		List<MantisServerSentEvent> origEvents = CompressionUtils.decompressAndBase64Decode(encodedString, true);
    //
    //		assertEquals(data.length, origEvents.size());
    //
    //		for(int i=0; i< origEvents.size(); i++) {
    //
    //			System.out.println("orig data -> " + data[i]);
    //
    //			System.out.println("deco data -> " + origEvents.get(i).getEventAsString());
    //
    //			assertEquals(data[i], origEvents.get(i).getEventAsString());
    //		}
    //
    //	}

    static List<String> generateData(int sz) {
        RandomString rs = new RandomString(200);
        List<String> eventList = new ArrayList<>();
        for (int i = 0; i < sz; i++) {
            eventList.add(rs.nextString());
        }
        return eventList;
    }

    public static void main(String[] args) {

        for (int i = 0; i < 20; i++) {
            RandomString rs = new RandomString(20);
            System.out.println("rs " + rs.nextString());
        }
    }

    @Test
    public void basicCompressDecompressSnappyTest() {
        List<String> eventList = generateData(100);
        boolean useSnappy = true;
        String encodedString = CompressionUtils.compressAndBase64Encode(eventList, useSnappy);

        List<MantisServerSentEvent> origEvents = CompressionUtils.decompressAndBase64Decode(encodedString, true, useSnappy);

        assertEquals(eventList.size(), origEvents.size());

        for (int i = 0; i < origEvents.size(); i++) {

            System.out.println("orig data -> " + eventList.get(i));

            System.out.println("deco data -> " + origEvents.get(i).getEventAsString());

            assertEquals(eventList.get(i), origEvents.get(i).getEventAsString());
        }

    }

    @Test
    public void basicCompressDecompressGzipTest() {
        List<String> eventList = generateData(100);
        String encodedString = CompressionUtils.compressAndBase64Encode(eventList);

        List<MantisServerSentEvent> origEvents = CompressionUtils.decompressAndBase64Decode(encodedString, true);

        assertEquals(eventList.size(), origEvents.size());

        for (int i = 0; i < origEvents.size(); i++) {

            System.out.println("orig data -> " + eventList.get(i));

            System.out.println("deco data -> " + origEvents.get(i).getEventAsString());

            assertEquals(eventList.get(i), origEvents.get(i).getEventAsString());
        }

    }

    static class RandomString {

        private static final char[] symbols;

        static {
            StringBuilder tmp = new StringBuilder();
            for (char ch = '0'; ch <= '9'; ++ch)
                tmp.append(ch);
            for (char ch = 'a'; ch <= 'z'; ++ch)
                tmp.append(ch);
            symbols = tmp.toString().toCharArray();
        }

        private final Random random = new Random();

        private final char[] buf;

        public RandomString(int length) {
            if (length < 1)
                throw new IllegalArgumentException("length < 1: " + length);
            buf = new char[length];
        }

        public String nextString() {
            for (int idx = 0; idx < buf.length; ++idx)
                buf[idx] = symbols[random.nextInt(symbols.length)];
            return new String(buf);
        }
    }


}
