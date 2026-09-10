package org.example;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;


public final class BitgetTradeProtocol {

    private static final URI WS_ENDPOINT = URI.create("wss://ws.bitget.com/v3/ws/public");

    private static final String INST_TYPE = "usdt-futures";
    private static final String TOPIC = "publicTrade";
    private static final String SUBSCRIBE = "subscribe";
    private static final String UNSUBSCRIBE = "unsubscribe";

    private static final String ERROR = "error";

    private static final String HEARTBEAT_MSG = "ping";
    private static final String HEARTBEAT_RESPONSE = "pong";

    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

    private final JsonMapper jsonMapper;

    public BitgetTradeProtocol(JsonMapper jsonMapper) {
        this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper must not be null");
    }

    public URI endpoint() { return WS_ENDPOINT; }
    public Duration heartbeatInterval() { return HEARTBEAT_INTERVAL; }
    public String heartbeatMessage() { return HEARTBEAT_MSG; }

    public String subscriptionMessage(String symbol) {
        return buildChannelMessage(SUBSCRIBE, normalizeSymbol(symbol));
    }

    public String unsubscriptionMessage(String symbol) {
        return buildChannelMessage(UNSUBSCRIBE, normalizeSymbol(symbol));
    }

    public boolean isPong(String msg) {
        return HEARTBEAT_RESPONSE.equals(msg);
    }

    public boolean isError(JsonNode node) {
        return ERROR.equals(node.path("event").asString(""));
    }

    public JsonNode parseJson(String msg) {
        try {
            return  jsonMapper.readTree(msg);
        }catch (JacksonException e){
            throw new IllegalArgumentException("Failed to parse Bitget WebSocket message", e);
        }
    }


    private String buildChannelMessage(String operation, String symbol) {

        var channel = new Channel(INST_TYPE, TOPIC, symbol);
        var request = new ChannelRequest(operation, List.of(channel));

        try{
            return jsonMapper.writeValueAsString(request);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to create Bitget WebSocket message", e);
        }

    }

    private String normalizeSymbol(String symbol) {
        Objects.requireNonNull(symbol, "symbol must not be null");
        if (symbol.isBlank()) { throw new IllegalArgumentException("symbol must not be blank"); }

        return symbol.trim().toUpperCase(Locale.ROOT);
    }

    private record Channel(String instType, String topic, String symbol) {}
    private record ChannelRequest(String op, List<Channel> args) {}

}



// Send Subscription Request
//{
//        "op": "subscribe",
//        "args": [
//        {
//            "instType": "usdt-futures",
//            "topic": "books1",
//            "symbol": "BTCUSDT"
//        }
//    ]
//}

//Subscription Confirmation

//{
//    "event": "subscribe",
//    "arg": {
//        "instType": "usdt-futures",
//        "topic": "books1",
//        "symbol": "BTCUSDT"
//    }
//}