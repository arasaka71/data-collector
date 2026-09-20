package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TradeFileWriter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TradeFileWriter.class);

    private static final int QUEUE_WARNING_PERCENT = 80;

    private static final int QUEUE_CAPACITY = 50_000;
    private static final int BATCH_SIZE = 500;
    private static final int WRITER_BUFFER_SIZE = 64 * 1024;
    private static final long FLUSH_INTERVAL_MILLIS = 1_000;

    private final BlockingQueue<JsonNode> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    private final AtomicBoolean acceptingMessages = new AtomicBoolean(true);
    private final AtomicReference<RuntimeException> writerFailure = new AtomicReference<>();

    private final Path file;
    private final BufferedWriter writer;

    private final ExecutorService writerExecutor;

    public TradeFileWriter(Path file) {
        this.file = file.toAbsolutePath();

        try {
            Path parent = this.file.getParent();

            if (parent != null) {
                Files.createDirectories(parent);
            }

            this.writer = new BufferedWriter(
                    new OutputStreamWriter(
                            Files.newOutputStream(
                                    this.file,
                                    StandardOpenOption.CREATE,
                                    StandardOpenOption.WRITE,
                                    StandardOpenOption.APPEND
                            ),
                            StandardCharsets.UTF_8
                    ),
                    WRITER_BUFFER_SIZE
            );
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to open trade file" + this.file, exception);
        }


        this.writerExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "bitget-trade-file-writer");
            thread.setDaemon(false);
            return thread;
        });

        this.writerExecutor.execute(this::runWriter);
    }

    public void enqueue(JsonNode message) {
        while (true) {
            throwIfWriterFailed();

            if (!acceptingMessages.get()) {
                throw new IllegalStateException("Trade file writer is already closed");
            }
            try {
                if (queue.offer(message, 100, TimeUnit.MILLISECONDS)) {
                    return;
                }

                // Ggf. hier einen Log einbauen: log.warn("Queue is full, backpressure building up!");
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while enqueueing trade message", exception);
            }
        }
    }

    private void runWriter() {
        List<JsonNode> batch  = new ArrayList<>(BATCH_SIZE);
        long lastFlushTime = System.nanoTime();
        long messagesSinceLastFlush = 0;
        long totalWrittenMessages = 0;

        LOGGER.info(
                "Trade file writer started: file={}, queueCapacity={}, batchSize={}, bufferSize={} bytes, flushInterval={} ms",
                file,
                QUEUE_CAPACITY,
                BATCH_SIZE,
                WRITER_BUFFER_SIZE,
                FLUSH_INTERVAL_MILLIS
        );

        try {
            while (acceptingMessages.get() || !queue.isEmpty()) {

                JsonNode firstMessage = queue.poll(100, TimeUnit.MILLISECONDS);
                if (firstMessage != null) {
                    batch.add(firstMessage);
                    queue.drainTo(batch, BATCH_SIZE -1);

                    int currentBatchSize = batch.size();

                    for (JsonNode message : batch) {
                        writer.write(message.toString());
                        writer.newLine();
                    }
                    batch.clear();

                    messagesSinceLastFlush += currentBatchSize;
                    totalWrittenMessages += currentBatchSize;
                }

                long now = System.nanoTime();
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(now - lastFlushTime);

                // Flush nur ausführen, wenn auch etwas Zeit vergangen ist ODER die Schleife gleich endet
                // || !acceptingMessages.get() && queue.isEmpty()
                if (elapsedMillis >= FLUSH_INTERVAL_MILLIS) {
                    writer.flush();

                    logFlushStatus(messagesSinceLastFlush, totalWrittenMessages);

                    messagesSinceLastFlush = 0;
                    lastFlushTime = now;
                }
            }
            // queue ist vollständig abgearbeitet
            writer.flush();

            LOGGER.info("Trade file writer drained and flushed: totalWritten={}, queueSize={}",
                    totalWrittenMessages,
                    queue.size()
            );
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            writerFailure.compareAndSet(null, new IllegalStateException("Writer thread interrupted", exception));
            LOGGER.error("Trade file writer interrupted: file={}", file, exception);
        } catch (IOException exception) {
            writerFailure.compareAndSet(null, new UncheckedIOException("Failed to write to:" + file, exception));
            LOGGER.error("Trade file writer failed: file={}", file, exception);
        } finally {
            acceptingMessages.set(false);
            try {
                writer.close();
            } catch (IOException exception) {
                writerFailure.compareAndSet(null, new UncheckedIOException("Failed to close file: " + file , exception));
                LOGGER.error("Failed to close trade file: file={}", file, exception);
            }
        }
    }

    private void throwIfWriterFailed() {
        RuntimeException failure = writerFailure.get();

        if (failure != null) {
            throw new IllegalStateException("Trade file writer has failed",failure);
        }
    }

    private void logFlushStatus(
            long flushedMessages,
            long totalWrittenMessages
    ) {
        int queueSize = queue.size();
        int remainingCapacity = QUEUE_CAPACITY - queueSize;
        int queueUsagePercent =
                (int) ((queueSize * 100L) / QUEUE_CAPACITY);

        if (queueUsagePercent >= QUEUE_WARNING_PERCENT) {
            LOGGER.warn(
                    "Trade queue backlog is high: flushed={}, totalWritten={}, queueSize={}/{}, queueUsage={}%, remainingCapacity={}",
                    flushedMessages,
                    totalWrittenMessages,
                    queueSize,
                    QUEUE_CAPACITY,
                    queueUsagePercent,
                    remainingCapacity
            );
        } else {
            LOGGER.debug(
                    "Trade data flushed: flushed={}, totalWritten={}, queueSize={}/{}, queueUsage={}%, remainingCapacity={}",
                    flushedMessages,
                    totalWrittenMessages,
                    queueSize,
                    QUEUE_CAPACITY,
                    queueUsagePercent,
                    remainingCapacity
            );
        }
    }

    @Override
    public void close() {
        acceptingMessages.set(false);
        writerExecutor.shutdown();
        try {
            while (!writerExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                // Writer verarbeitet weiter die restliche Queue
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while closing TradeFileWriter", exception);
        }

        throwIfWriterFailed();
    }
}

