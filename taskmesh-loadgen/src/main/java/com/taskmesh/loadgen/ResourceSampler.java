package com.taskmesh.loadgen;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;

/**
 * Generator-side resource usage.
 * <p>
 * Reports only what the JVM can actually observe about itself. Anything it
 * cannot measure reliably is reported as {@code NOT MEASURED} rather than
 * estimated - an invented resource number would undermine the benchmark it
 * is supposed to support.
 * <p>
 * Process RSS in particular is <strong>not</strong> obtainable from inside
 * the JVM. {@code getCommittedVirtualMemorySize()} is sometimes mistaken
 * for it but measures committed virtual address space, which is far larger
 * and means something different, so it is reported under its own name.
 * Real RSS has to be read from outside the process.
 */
public final class ResourceSampler {

    private final com.sun.management.OperatingSystemMXBean osBean = resolveOsBean();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    private static com.sun.management.OperatingSystemMXBean resolveOsBean() {
        var bean = ManagementFactory.getOperatingSystemMXBean();
        return bean instanceof com.sun.management.OperatingSystemMXBean sun ? sun : null;
    }

    /**
     * Process CPU load as a fraction of total machine capacity, or -1 when
     * the platform bean is unavailable.
     */
    public double processCpuLoad() {
        return osBean == null ? -1 : osBean.getProcessCpuLoad();
    }

    /** Committed virtual address space - NOT resident set size. -1 when unavailable. */
    public long committedVirtualMemoryBytes() {
        if (osBean == null) {
            return -1;
        }
        try {
            return osBean.getCommittedVirtualMemorySize();
        } catch (UnsupportedOperationException e) {
            return -1;
        }
    }

    public long heapUsedBytes() {
        return memoryBean.getHeapMemoryUsage().getUsed();
    }

    public long nonHeapUsedBytes() {
        return memoryBean.getNonHeapMemoryUsage().getUsed();
    }

    /** Platform threads only - virtual threads are deliberately not counted here. */
    public int platformThreadCount() {
        return threadBean.getThreadCount();
    }

    /**
     * The carrier pool size virtual threads are multiplexed onto. This is the
     * number that matters when arguing that N logical workers do not cost N
     * OS threads.
     */
    public int carrierParallelism() {
        String configured = System.getProperty("jdk.virtualThreadScheduler.parallelism");
        if (configured != null) {
            try {
                return Integer.parseInt(configured);
            } catch (NumberFormatException ignored) {
                // fall through to the default
            }
        }
        return Runtime.getRuntime().availableProcessors();
    }

    public int availableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static String formatCpu(double load) {
        return load < 0 ? "NOT MEASURED" : "%.1f%%".formatted(load * 100);
    }

    public static String formatBytes(long bytes) {
        return bytes < 0 ? "NOT MEASURED" : "%.1f MiB".formatted(bytes / (1024.0 * 1024.0));
    }
}
