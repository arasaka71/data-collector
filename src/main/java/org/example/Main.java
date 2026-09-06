package org.example;


import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

@SpringBootApplication
public class Main {
    static void main(String[] args){ SpringApplication.run(Main.class, args); }


    @Bean
    CommandLineRunner run(BitgetClient bitgetClient){
        return args -> {

            var candles = bitgetClient.getHistoricalCandles(
                    "BTCUSDT",
                    "1m",
                    Instant.now().minus(Duration.ofHours(6)),
                    Instant.now()
            );

            Path path = Path.of("candles.csv");

            StringBuilder csv = new StringBuilder();

            csv.append("timestamp,open,high,low,close,volume, turnover\n");

            for (Candle candle : candles) {
                csv.append(candle.timestamp()).append(",")
                        .append(candle.open()).append(",")
                        .append(candle.high()).append(",")
                        .append(candle.low()).append(",")
                        .append(candle.close()).append(",")
                        .append(candle.volume()).append(",")
                        .append(candle.turnover()).append("\n");
            }

            Files.writeString(path, csv.toString());
            IO.println("Data written to " + path.toAbsolutePath());
        };
    }
}
