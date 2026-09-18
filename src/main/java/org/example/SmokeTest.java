package org.example;

import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public final class SmokeTest {
     static void main(String[] args) throws InterruptedException {
        String btcAsset = "BTCUSDT";

        //var firstTradeReceived = new CompletableFuture<Void>();

        //ObjectReader reader = jsonMapper.reader();
        //ObjectWriter writer = jsonMapper.writer();
        // JsonMapper.builder().findAndAddModules()

        var jsonMapper = JsonMapper.builder().build();

        var protocol = new BitgetTradeProtocol(jsonMapper);
        var decoder = new MessageDecoder(jsonMapper);
        var tradeFileWriter = new TradeFileWriter(Path.of("data", "bitget-trades.jsonl"));

        var webSocketConnection = new WebSocketConnection();


        var receiver = new BitgetTradeReceiver(webSocketConnection, protocol, decoder,  tradeFileWriter);

        try{
            receiver.connect().toCompletableFuture().join();
            IO.println("CONNECTED");

            receiver.subscribe(btcAsset).join();

            IO.println("SUBCRIBTION SENT");

            Thread.sleep(30_000);

        } finally {
            //receiver.disconnect().join();
            tradeFileWriter.close();
        }

    }
}
