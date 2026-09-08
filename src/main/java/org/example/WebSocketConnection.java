package org.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public final class WebSocketConnection implements WebSocket.Listener {

    private final HttpClient httpClient;
    private final Consumer<String> messageHandler;

    private final StringBuilder textBuffer = new StringBuilder();

    private final Object sendLock = new Object();
    private CompletableFuture<Void> sendTail = CompletableFuture.completedFuture(null);

    private volatile WebSocket webSocket;

    public WebSocketConnection(Consumer<String> messageHandler) {
        this.httpClient = HttpClient.newHttpClient();
        this.messageHandler = messageHandler;
    }


    public CompletionStage<Void> connect(URI endpoint) {
        return httpClient.newWebSocketBuilder()
                .buildAsync(endpoint, this)
                .thenAccept(socket -> this.webSocket = socket);

    }


    @Override
    public void onOpen(WebSocket webSocket) {
        this.webSocket = webSocket;
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

    public CompletableFuture<Void> sendText(String message) {
        Objects.requireNonNull(message);

        WebSocket socket = requireConnectedWebSocket();

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

    private WebSocket requireConnectedWebSocket() {
        WebSocket socket = this.webSocket;

        if (socket == null
            || socket.isInputClosed()
            || socket.isOutputClosed()) {
            throw new IllegalStateException("Cannot connect to websocket");
        }

        return socket;
    }




//    public void connect(URI endpoint) {
//
//    }
//
//    public void disconnect() {
//
//    }
//
//    @Override
//    public void onOpen(WebSocket webSocket) {
//
//    }
//
//    @Override
//    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
//        return null;
//    }
//
//    @Override
//    public void onError(WebSocket webSocket, Throwable error) {
//
//    }
//
//    @Override
//    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
//        return null;
//    }

}
