package org.example;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.Objects;

public final class  MessageDecoder {

    private static final String PONG = "pong";
    private static final String SUBSCRIBE = "subscribe";
    private static final String UNSUBSCRIBE = "unsubscribe";
    private static final String ERROR = "error";
    private static final String SNAPSHOT = "snapshot";
    private static final String UPDATE = "update";

    private final JsonMapper jsonMapper;

    public MessageDecoder(JsonMapper jsonMapper) {
        this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper must not be null");
    }

    public ClassifiedMessage decode(String rawMessage){
        if (PONG.equals(rawMessage)){
          return new ClassifiedMessage(MessageType.PONG, null);
        }

        JsonNode root =  parseJson(rawMessage);
        MessageType type = classifyMessageType(root);

        return new ClassifiedMessage(type, root);
    }

    private MessageType classifyMessageType(JsonNode root){
        String event = root.path("event").asString("");

        return switch (event){
            case "" -> classifyAction(root);
            case SUBSCRIBE ->  MessageType.SUBSCRIPTION_ACK;
            case UNSUBSCRIBE ->  MessageType.UNSUBSCRIPTION_ACK;
            case ERROR ->  MessageType.ERROR;
            default ->  MessageType.UNKNOWN;
        };
    }

    private MessageType classifyAction(JsonNode root){
        String action = root.path("action").asString("");
        return switch (action){
            case UPDATE -> MessageType.TRADE_UPDATE;
            case SNAPSHOT -> MessageType.TRADE_SNAPSHOT;
            default -> MessageType.UNKNOWN;
        };
    }

    private JsonNode parseJson(String msg){
        try{
            return jsonMapper.readTree(msg);
        }catch (JacksonException e){
            throw new IllegalArgumentException("Failed to parse message", e);
        }
    }
}

//{
//    "action":"update",
//    "arg": {
//        "instType":"usdt-futures",
//        "topic":"publicTrade",
//        "symbol":"BTCUSDT"
//    },
//        "data":[
//                {
//                    "i":"1483419369733898240",
//                    "p":"78543.2",
//                    "v":"0.0054",
//                    "S":"buy",
//                    "T":"1789400659020",
//                    "L":"1483419369733898241",
//                    "isRPI":"no"
//                }
//              ],
//        "ts":1789400659022
//}