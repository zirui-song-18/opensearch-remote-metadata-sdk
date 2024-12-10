/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import java.security.PrivilegedAction;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static org.opensearch.common.util.concurrent.ThreadContextAccess.doPrivileged;

public abstract class AbstractSdkClient implements SdkClientDelegate {

    protected <T> CompletionStage<T> executePrivilegedAsync(PrivilegedAction<T> action, Executor executor) {
        return CompletableFuture.supplyAsync(() -> doPrivileged(action), executor);
    }
}
