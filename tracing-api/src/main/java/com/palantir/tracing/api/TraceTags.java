/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.tracing.api;

/**
 * A set of standard tracing tags meant to be reused where applicable. These are based on the
 * <a href="https://docs.datadoghq.com/logs/processing/attributes_naming_convention/#default-standard-attribute-list">
 * Datadog APM standard attribute list</a>.
 *
 * The values of these attributes may change without warning, it's best to use this utility class to standardize
 * with other services.
 */
public final class TraceTags {

    // Network
    // The following attributes are related to the data used in network communication..

    // ip addresses may not always be safe, so these tags are currently
    // excluded out of caution.
    // /** The IP address of the client that initiated the TCP connection. */
    // public static final String NETWORK_CLIENT_IP = "network.client.ip";
    // /** The IP address the client connected to. */
    // public static final String NETWORK_DESTINATION_IP = "network.destination.ip";
    /** The port of the client that initiated the connection. */
    public static final String NETWORK_CLIENT_PORT = "network.client.port";
    /** The TCP port the client connected to. */
    public static final String NETWORK_DESTINATION_PORT = "network.destination.port";
    /** Total number of bytes transmitted from the client to the server when the log is emitted. */
    public static final String NETWORK_BYTES_READ = "network.bytes_read";
    /** Total number of bytes transmitted from the server to the client when the log is emitted. */
    public static final String NETWORK_BYTES_WRITTEN = "network.bytes_written";

    // HTTP Requests
    // These attributes are related to the data commonly used in HTTP requests and accesses.

    // Common Attributes

    // 'http.url' is currently excluded to prevent leaking sensitive data
    // /** The URL of the HTTP request. */
    // public static final String HTTP_URL = "http.url";
    /** The HTTP response status code. */
    public static final String HTTP_STATUS_CODE = "http.status_code";
    /** Indicates the desired action to be performed for a given resource. */
    public static final String HTTP_METHOD = "http.method";
    /** HTTP header field that identifies the address of the webpage that linked to the resource being requested. */
    public static final String HTTP_REFERER = "http.referer";
    /** The ID of the HTTP request. */
    public static final String HTTP_REQUEST_ID = "http.request_id";
    /** The User-Agent as it is sent (raw format). */
    public static final String HTTP_USER_AGENT = "http.useragent";
    /** The User-Agent propagated across service boundaries as it is sent (raw format). */
    public static final String HTTP_FOR_USER_AGENT = "http.for_useragent";
    /** The version of HTTP used for the request. */
    public static final String HTTP_VERSION = "http.version";

    // URL details attributes
    // Note that the 'http.url_details.queryString' is missing because it cannot be described as a single string value.
    // The data would likely be unsafe to share anyhow.

    // 'http.url_details.host' may contain unsafe data and is currently excluded out of caution.
    // /** The HTTP host part of the URL. */
    // public static final String HTTP_URL_HOST = "http.url_details.host";
    /** The HTTP port part of the URL. */
    public static final String HTTP_URL_PORT = "http.url_details.port";
    /**
     * The HTTP path part of the URL.
     * When setting this tag, the caller takes responsibility for redaction, ensuring the path is a known
     * template which does not include user-provided path parameter data.
     */
    public static final String HTTP_URL_PATH_TEMPLATE = "http.url_details.path";
    /** The protocol name of the URL (HTTP or HTTPS). */
    public static final String HTTP_URL_SCHEME = "http.url_details.scheme";

    // User-Agent attributes
    /** The OS family reported by the User-Agent. */
    public static final String HTTP_USER_AGENT_OS_FAMILY = "http.useragent_details.os.family";
    /** The Browser Family reported by the User-Agent. */
    public static final String HTTP_USER_AGENT_BROWSER_FAMILY = "http.useragent_details.browser.family";
    /** The Device family reported by the User-Agent. */
    public static final String HTTP_USER_AGENT_DEVICE_FAMILY = "http.useragent_details.device.family";

    private TraceTags() {}
}
