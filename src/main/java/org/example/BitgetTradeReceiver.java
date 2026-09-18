package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class BitgetTradeReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(BitgetTradeReceiver.class);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            runnable -> {
                Thread thread = new Thread(runnable, "bitget-lifecycle");
                thread.setDaemon(true);
                return thread;
            }
    );

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduler = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);

    private final AtomicBoolean awaitingPong = new AtomicBoolean(false);

    private volatile ScheduledFuture<?> heartbeatTask;
    private volatile ScheduledFuture<?> reconnectTask;

    private final Set<String> desiredSubscriptions = ConcurrentHashMap.newKeySet();

    private final WebSocketConnection websocket;
    private final BitgetTradeProtocol protocol;
    private final MessageDecoder messageDecoder;


    public BitgetTradeReceiver(WebSocketConnection connection, BitgetTradeProtocol protocol, MessageDecoder messageDecoder) {
        this.websocket = connection;
        this.protocol = protocol;
        this.messageDecoder = messageDecoder;
    }

    public CompletionStage<Void> connect() {
        shuttingDown.set(false);
        return openConnection();
    }

    private CompletionStage<Void> openConnection() {
        return websocket.connect(
                protocol.endpoint(),
                this::handleMessage, //message -> this.handleMessage(message)
                this::handleConnectionError,
                this::handleConnectionClosed
        ).whenComplete((unused, error) -> {
            if (error == null) {
                reconnectAttempt.set(0);
                reconnectScheduler.set(false);
                LOGGER.info("WebSocket connection established");

                resubscribeDesiredSubscriptions();
                startHeartbeat();
            } else {
                LOGGER.error("WebSocket connection attempt failed", error);

                scheduleReconnect();
            }
        });
    }

    public CompletableFuture<Void> subscribe(String symbol) {
        String normalizedSymbol = protocol.normalizeSymbol(symbol);
        String msg = protocol.subscriptionMessage(normalizedSymbol);

        desiredSubscriptions.add(normalizedSymbol);

        return websocket.sendText(msg);
    }

    public CompletableFuture<Void> unsubscribe(String symbol) {
        String normalizedSymbol = protocol.normalizeSymbol(symbol);
        String msg = protocol.unsubscriptionMessage(normalizedSymbol);

        desiredSubscriptions.remove(normalizedSymbol);

        return websocket.sendText(msg);
    }

    public CompletableFuture<Void> disconnect() {
        shuttingDown.set(true);
        stopHeartbeat();

        ScheduledFuture<?> task = this.reconnectTask;

        if (task != null) {
            task.cancel(false);
        }
        reconnectScheduler.set(false);
        return websocket.disconnect();
    }

    private void resubscribeDesiredSubscriptions() {
        for (String symbol : desiredSubscriptions) {
            try {
                websocket.sendText(protocol.subscriptionMessage(symbol))
                        .whenComplete((unused, error) -> {
                            if (error == null) {
                                LOGGER.info("Resubscription send for symbol={}", symbol);
                            } else {
                                LOGGER.error("Failed to send Resubscription for symbol={}", symbol, error);
                            }
                        });
            } catch (RuntimeException exception) {
                LOGGER.error("Could not send Resubscription for symbol={}", symbol, exception);
            }
        }
    }

    private void scheduleReconnect() {
        if (shuttingDown.get() || websocket.isConnected()) {
            return;
        }

        if (!reconnectScheduler.compareAndSet(false, true)) {
            return;
        }

        int attempt = reconnectAttempt.incrementAndGet();
        long delaySeconds = Math.min(30, 1L << Math.min(attempt - 1, 5));

        LOGGER.warn("Scheduling reconnect attempt {} in {} seconds}", attempt, delaySeconds);

        reconnectTask = scheduler.schedule(() ->
                {
                    reconnectScheduler.set(false);
                    if (shuttingDown.get()) {
                        return;
                    }
                    openConnection();
                },
                delaySeconds,
                TimeUnit.SECONDS
        );
    }

    private void startHeartbeat() {
        stopHeartbeat();

        long intervalMillis = protocol.heartbeatInterval().toMillis();

        heartbeatTask = scheduler.scheduleAtFixedRate(
                this::sendHeartbeat,
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    private void stopHeartbeat() {
        ScheduledFuture<?> task = heartbeatTask;

        if (task != null) {
            task.cancel(false);
            heartbeatTask = null;
        }
        awaitingPong.set(false);
    }

    private void sendHeartbeat() {
        if (shuttingDown.get() || !websocket.isConnected()) {
            return;
        }

        if (!awaitingPong.compareAndSet(false, true)) {
            if (awaitingPong.compareAndSet(true, false)) {
                handleHeartbeatFailure("No pong received before the next heartbeat", null);
            }
            return;
        }

        try {
            websocket.sendText(protocol.heartbeatMessage())
                    .whenComplete((unused, error) -> {
                        if (error != null) {
                            handleHeartbeatFailure("Failed to send heartbeat", error);
                        }
                    });
        } catch (RuntimeException exception) {
            handleHeartbeatFailure("Failed to send heartbeat", exception);
        }
    }

    private void handleHeartbeatFailure(String message, Throwable error) {
        if (shuttingDown.get()) {return;}
        if (error == null) {
            LOGGER.warn(message);
        } else {
            LOGGER.error(message, error);
        }

        stopHeartbeat();
        // websocket.abort();
        scheduleReconnect();
    }


    private void handleConnectionClosed(Integer status, String reason) {
        stopHeartbeat();

        if (shuttingDown.get()) {
            LOGGER.warn("WebSocket connection closed: statusCode={}, reason={}", status, reason);
            return;
        }

        LOGGER.warn("WebSocket connection closed unexpectedly: statusCode={}, reason={}", status, reason);
        scheduleReconnect();
    }

    private void handleConnectionError(Throwable error) {
        stopHeartbeat();
        if (shuttingDown.get()) {return;}

        LOGGER.error("WebSocket connection error", error);
        scheduleReconnect();
    }


    private void handlePong(){
        // Wird später für die Heartbeat-Überwachung benötigt.
        if (awaitingPong.getAndSet(false)) {
            LOGGER.debug("Heartbeat pong received");
        }
    }

    private void handleMessage(String rawMessage){
        ClassifiedMessage msg;

        try {
            msg = messageDecoder.decode(rawMessage);
        }catch (IllegalArgumentException exception){
            handleInvalidMessage(rawMessage, exception);
            return;
        }

        switch (msg.type()){
            case PONG -> handlePong();
            case TRADE_UPDATE  -> handleTradeUpdate(msg.jsonTree());
            case TRADE_SNAPSHOT -> handleTradeSnapshot(msg.jsonTree());
            case SUBSCRIPTION_ACK -> handleSubscriptionAcknowledgement(msg.jsonTree());
            case UNSUBSCRIPTION_ACK -> handleUnsubscriptionAcknowledgement(msg.jsonTree());
            case ERROR -> handleErrorMessage(msg.jsonTree());
            case UNKNOWN -> handleUnknownMessage(msg.jsonTree());
        }
    }

    private void handleSubscriptionAcknowledgement(JsonNode message){
        // Subscription wurde von Bitget bestätigt.
    }

    private void handleUnsubscriptionAcknowledgement(JsonNode message){
        // Unsubscription wurde von Bitget bestätigt.
    }

    private void handleErrorMessage(JsonNode message){
        // Bitget hat eine Fehlermeldung gesendet.
    }

    private void handleTradeSnapshot(JsonNode message){
        // Wird später an die Trade-Verarbeitung weitergegeben.
    }

    private void handleTradeUpdate(JsonNode message){
        // Wird später an die Trade-Verarbeitung weitergegeben.
    }

    private void handleUnknownMessage(JsonNode message){
        // Unbekannte Nachricht protokollieren.
    }

    private void handleInvalidMessage(String rawMessage, Exception exception){
        // Ungültige Nachricht protokollieren.
    }
}
