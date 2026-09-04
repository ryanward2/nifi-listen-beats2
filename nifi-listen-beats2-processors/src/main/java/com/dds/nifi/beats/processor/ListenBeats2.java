/*
 * Copyright 2026 DDS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dds.nifi.beats.processor;

import com.dds.nifi.beats.api.BatchClaim;
import com.dds.nifi.beats.api.BeatsListenerService;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "beats", "lumberjack", "filebeat", "winlogbeat", "elastic-agent", "ndjson"})
@CapabilityDescription("Drains committed-ready Beats batches from a Beats Listener Service, writes NDJSON FlowFiles, commits the NiFi session, then permits cumulative Beats ACKs.")
public final class ListenBeats2 extends AbstractProcessor {
    public static final PropertyDescriptor LISTENER_SERVICE = new PropertyDescriptor.Builder()
            .name("beats-listener-service").displayName("Beats Listener Service")
            .description("Controller Service that owns the Netty listener, queues, protocol state, and ACK coordination.")
            .required(true).identifiesControllerService(BeatsListenerService.class).build();
    public static final PropertyDescriptor MAX_BATCHES = new PropertyDescriptor.Builder()
            .name("maximum-batches-per-trigger").displayName("Maximum Batches per Trigger")
            .description("Maximum batch claims in one explicit NiFi transaction.").required(true).defaultValue("32")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor MAX_EVENTS = new PropertyDescriptor.Builder()
            .name("maximum-events-per-trigger").displayName("Maximum Events per Trigger")
            .description("Maximum events in one explicit NiFi transaction.").required(true).defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor MAX_BYTES = new PropertyDescriptor.Builder()
            .name("maximum-bytes-per-trigger").displayName("Maximum Bytes per Trigger")
            .description("Maximum raw event bytes in one explicit NiFi transaction.").required(true).defaultValue("64 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR).build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success").description("Successfully committed NDJSON batches.").build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(LISTENER_SERVICE, MAX_BATCHES, MAX_EVENTS, MAX_BYTES);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final BeatsListenerService service = context.getProperty(LISTENER_SERVICE).asControllerService(BeatsListenerService.class);
        final int maximumBatches = context.getProperty(MAX_BATCHES).asInteger();
        final int maximumEvents = context.getProperty(MAX_EVENTS).asInteger();
        final long maximumBytes = context.getProperty(MAX_BYTES).asDataSize(DataUnit.B).longValue();
        final List<BatchClaim> claims = service.claimBatches(maximumBatches, maximumEvents, maximumBytes);
        if (claims.isEmpty()) {
            context.yield();
            return;
        }

        final List<UUID> claimIds = new ArrayList<>(claims.size());
        boolean sessionCommitted = false;
        try {
            for (BatchClaim claim : claims) {
                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, rawOutput -> {
                    final BufferedOutputStream output = new BufferedOutputStream(rawOutput, 64 * 1024);
                    for (var content : claim.contents()) {
                        content.writeTo(output);
                    }
                    output.flush();
                });
                flowFile = session.putAllAttributes(flowFile, claim.attributes());
                session.getProvenanceReporter().receive(flowFile, claim.transitUri());
                session.transfer(flowFile, SUCCESS);
                session.adjustCounter("ListenBeats2 FlowFiles", 1, false);
                session.adjustCounter("ListenBeats2 Events", claim.eventCount(), false);
                session.adjustCounter("ListenBeats2 Payload Bytes", claim.uncompressedBytes(), false);
                claimIds.add(claim.claimId());
            }

            // Deliberate explicit transaction boundary. This processor is not annotated with @SupportsBatching.
            session.commit();
            sessionCommitted = true;
        } catch (Exception e) {
            if (!sessionCommitted) {
                try {
                    session.rollback();
                } finally {
                    service.rollbackClaims(claims.stream().map(BatchClaim::claimId).toList());
                }
            }
            throw new ProcessException("Failed to commit Beats batch transaction", e);
        }

        // Never roll back a NiFi transaction that has already committed. A receipt failure leaves the events
        // unacknowledged, so Beats retries and may create duplicates instead of risking silent data loss.
        try {
            service.commitClaims(claimIds);
        } catch (RuntimeException e) {
            // The NiFi transaction is already committed and must never be rolled back or reported as failed.
            // Leaving the Beats data unacknowledged causes an at-least-once retry instead of silent loss.
            getLogger().error("NiFi committed Beats batches but post-commit ACK finalization failed; duplicates are possible", e);
        }
    }
}
