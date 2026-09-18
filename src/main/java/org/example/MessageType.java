package org.example;

public enum MessageType {
    PONG,
    SUBSCRIPTION_ACK,
    UNSUBSCRIPTION_ACK,
    TRADE_SNAPSHOT,
    TRADE_UPDATE,
    ERROR,
    UNKNOWN
}
