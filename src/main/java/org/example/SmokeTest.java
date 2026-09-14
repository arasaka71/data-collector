package org.example;

import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;

public final class SmokeTest {
     static void main(String[] args) throws InterruptedException {
        //var firstTradeReceived = new CompletableFuture<Void>();

        var protocal = new BitgetTradeProtocol(JsonMapper.builder().build());

         //                    if(protocal.isPong(message)) {
         //                        return;
         //                    }
         //
         //                    var json = protocal.parseJson(message);
         //
         //                    if (json.has("data")){
         //                        firstTradeReceived.complete(null);
         //                    }
         var connection = new WebSocketConnection(
                 IO::println,

                error -> {
                    IO.println("ERROR: " + error.getMessage());
                },

                (statusCode, reason) -> {
                    IO.println("STATUS: " + statusCode);
                }
        );


        var receiver = new BitgetTradeReceiver(connection, protocal);

        try{
            receiver.connect().toCompletableFuture().join();
            IO.println("CONNECTED");

            receiver.subscribe("BTCUSDT").join();

            IO.println("SUBCRIBTION SENT");

            Thread.sleep(30_000);

        } finally {
            receiver.disconnect().join();
        }

    }
}
