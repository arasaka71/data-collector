package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SmokeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmokeTest.class);
    private static final String BTC_ASSET = "BTCUSDT";

     static void main(String[] args) {
        //ObjectReader reader = jsonMapper.reader();
        //ObjectWriter writer = jsonMapper.writer();
        // JsonMapper.builder().findAndAddModules()

        var jsonMapper = JsonMapper.builder().build();

        var protocol = new BitgetTradeProtocol(jsonMapper);
        var decoder = new MessageDecoder(jsonMapper);
        var tradeFileWriter = new TradeFileWriter(Path.of("data", "bitget-trades.jsonl"));

        var webSocketConnection = new WebSocketConnection();

        var bitgetClient = new BitgetTradeReceiver(webSocketConnection, protocol, decoder, tradeFileWriter);

        var keepAlive = new CountDownLatch(1);
         Runnable shutdown = getShutdown(bitgetClient, tradeFileWriter, keepAlive);

         Runtime.getRuntime().addShutdownHook(new  Thread(shutdown, "bitget-smoke-test-shutdown"));

         bitgetClient.subscribe(BTC_ASSET).join();

         bitgetClient.connect();

         try {
             keepAlive.await();
         } catch (InterruptedException exception) {
             Thread.currentThread().interrupt();
             LOGGER.error("Bitget smoke test main thread interrupted", exception);
         } finally {
             shutdown.run();
         }
    }

    private static Runnable getShutdown(BitgetTradeReceiver receiver, TradeFileWriter tradeFileWriter, CountDownLatch keepAlive) {
        var shutdownStarted = new AtomicBoolean(false);

        Runnable shutdown = () -> {
            if (!shutdownStarted.compareAndSet(false, true)) {
                return;
            }
            LOGGER.info("Stopping Bitget smoke test");

            try {
                receiver.disconnect().join();
            } catch (CompletionException exception) {
                LOGGER.error("Failed to disconnect WebSocket cleanly", exception);
            }

            try {
                tradeFileWriter.close();
            } catch (RuntimeException exception) {
                LOGGER.error("Failed to close TradeFileWriter cleanly", exception);
            } finally {
                keepAlive.countDown();
            }

            LOGGER.info("Bitget smoke test stopped");
        };
        return shutdown;
    }
}
