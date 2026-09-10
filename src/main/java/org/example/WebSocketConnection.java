package org.example;

import javax.xml.datatype.Duration;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class WebSocketConnection implements WebSocket.Listener {

    private final HttpClient httpClient;
    private final Consumer<String> messageHandler;
    private final Consumer<Throwable> errorHandler;
    private final BiConsumer<Integer, String> closeHandler;

    private final StringBuilder textBuffer = new StringBuilder();
    private final AtomicBoolean connecting = new AtomicBoolean(false);

    private final Object sendLock = new Object();
    private CompletableFuture<Void> sendTail = CompletableFuture.completedFuture(null);

    private volatile WebSocket webSocket;

    public WebSocketConnection(Consumer<String> messageHandler,  Consumer<Throwable> errorHandler,  BiConsumer<Integer, String> closeHandler) {
        this.httpClient = HttpClient.newHttpClient();
        this.messageHandler = messageHandler; // Objects.requireNonNull()
        this.errorHandler = errorHandler;
        this.closeHandler = closeHandler;
    }


    public CompletionStage<Void> connect(URI endpoint) {
        Objects.requireNonNull(endpoint, "Endpoint must not be null");

        if (isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Already connected"));
        }
        if (!connecting.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(new IllegalStateException("Already connected"));
        }

        return httpClient.newWebSocketBuilder()
                //.connectTimeout(CONNECT_TIMEOUT) ev
                .buildAsync(endpoint, this)
                .thenAccept(socket -> this.webSocket = socket)
                .whenComplete((unused, error) -> connecting.set(false));
    }

    public CompletableFuture<Void> sendText(String message) {
        Objects.requireNonNull(message);
        WebSocket socket = requireConnectedWebSocket();

        // chains the new send operation to sendTail
        // the next socket.sendText() call starts only after the previous send operation has completed
        synchronized (sendLock) {
            CompletableFuture<Void> sendFuture = sendTail
                    .handle((unused, previousError) -> null)
                    .thenCompose(unused ->
                            socket.sendText(message, true)
                    )
                    .thenApply(unused -> null);

            sendTail = sendFuture;

            return sendFuture;
        }
    }


    public CompletableFuture<Void> disconnect() {
        WebSocket socket = this.webSocket;

        if (socket == null || socket.isOutputClosed()) {
            return CompletableFuture.completedFuture(null);
        }

        synchronized (sendLock) {
            CompletableFuture<Void> closeFuture = sendTail
                    .handle((unused, previousError) -> null)
                    .thenCompose(unused -> socket.sendClose(WebSocket.NORMAL_CLOSURE, "")
                    )
                    .thenApply(unused -> null);

            sendTail = closeFuture;
            return closeFuture;
        }
    }

    public boolean isConnected() {
        WebSocket socket = this.webSocket;

        return socket != null
                && !socket.isInputClosed()
                && !socket.isOutputClosed();

    }

    @Override
    public void onOpen(WebSocket webSocket) {
        this.webSocket = webSocket;

        synchronized (sendLock) {
            sendTail = CompletableFuture.completedFuture(null);
        }

        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        try {
            textBuffer.append(data);

            if (last) {
                String msg = textBuffer.toString();
                textBuffer.setLength(0);
                messageHandler.accept(data.toString());
            }
            return null;
        } finally {
            webSocket.request(1);
        }
    }

    // hier eigendlich schdeuler für reconnect
    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        clearConnection(webSocket);
        errorHandler.accept(error);
    }

    @Override
    public CompletableFuture<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        clearConnection(webSocket);
        closeHandler.accept(statusCode, reason);

        return null;
    }

    private WebSocket requireConnectedWebSocket() {
        WebSocket socket = this.webSocket;

        if (socket == null
            || socket.isInputClosed()
            || socket.isOutputClosed()) {
            throw new IllegalStateException("Cannot connect to websocket");
        }

        return socket;
    }

    private void clearConnection(WebSocket socket) {
        if (this.webSocket == socket) {
            this.webSocket = null;

            textBuffer.setLength(0);
        }
    }
}
