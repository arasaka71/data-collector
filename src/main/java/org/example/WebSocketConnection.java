package org.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public final class WebSocketConnection implements WebSocket.Listener {

    private final HttpClient httpClient;
    private final Consumer<String> messageHandler;

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
        messageHandler.accept(data.toString());

        webSocket.request(1);

        return null;
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
