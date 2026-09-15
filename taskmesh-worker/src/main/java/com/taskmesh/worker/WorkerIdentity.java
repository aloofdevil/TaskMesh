package com.taskmesh.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * This worker's identity. The id must be stable across a restart of the
 * same worker (so it re-registers as itself) and unique across workers (so
 * two replicas never share a row), which is why it is taken from
 * configuration when provided - in Kubernetes that will be the pod name -
 * and only generated as a fallback.
 */
@Component
public class WorkerIdentity {

    private final String workerId;
    private final String hostname;

    public WorkerIdentity(WorkerProperties properties) {
        this.hostname = resolveHostname();
        this.workerId = StringUtils.hasText(properties.id())
                ? properties.id()
                : hostname + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-host";
        }
    }

    public String workerId() {
        return workerId;
    }

    public String hostname() {
        return hostname;
    }
}
