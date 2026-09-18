package org.example;

import tools.jackson.databind.JsonNode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class TradeFileWriter implements AutoCloseable {

    private final Path file;
    private final BufferedWriter writer;

    public TradeFileWriter(Path file) {
        this.file = file.toAbsolutePath();

        try {
            Path parent = this.file.getParent();

            if (parent != null) {
                Files.createDirectories(parent);
            }

            this.writer = Files.newBufferedWriter(
                    this.file,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to open trade file" + this.file, exception);
        }
    }


    public synchronized void write(JsonNode message) {
        try {
            writer.write(message.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to write trade message to" + this.file, exception);
        }
    }

    @Override
    public synchronized void close() {
        try {
            writer.close();
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to close trade file" + this.file, exception);
        }
    }
}

