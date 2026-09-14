package org.example;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class BitgetTradeReceiver {
    private final WebSocketConnection connection;
    private final BitgetTradeProtocol protocol;

    public BitgetTradeReceiver(WebSocketConnection connection, BitgetTradeProtocol protocol) {
        this.connection = connection;
        this.protocol = protocol;
    }

    public CompletionStage<Void> connect() {
        return connection.connect(protocol.endpoint());
    }

    public CompletableFuture<Void> subscribe(String symbole) {
        String msg = protocol.subscriptionMessage(symbole);
        return connection.sendText(msg);
    }

    public CompletableFuture<Void> unsubscribe(String symbole) {
        String msg = protocol.unsubscriptionMessage(symbole);
        return connection.sendText(msg);
    }

    public CompletableFuture<Void> disconnect() {
        return connection.disconnect();
    }
}
