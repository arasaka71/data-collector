package org.example;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Component
public class BitgetClient {
    private final RestClient restClient;

    public BitgetClient(RestClient.Builder builder) {
        this.restClient = builder
                .baseUrl("https://api.bitget.com")
                .build();
    }

    public List<Candle>getHistoricalCandles(
            String symbol,
            String interval,
            Instant startTime,
            Instant endTime
    ) {
        BitgetResponse<List<List<String>>> response = restClient.get()
                .uri(uriBuilder -> uriBuilder
                    .path("/api/v3/market/history-candles")
                    .queryParam("category", "USDT-FUTURES")
                    .queryParam("symbol", symbol)
                    .queryParam("interval", interval)
                    .queryParam("startTime", startTime.toEpochMilli())
                    .queryParam("type", "market")
                    .queryParam("endTime", endTime.toEpochMilli())
                    .queryParam("limit", 100)
                    .build())
                .retrieve()
                .body(new ParameterizedTypeReference<BitgetResponse<List<List<String>>>>() {});

        return mapCandles(response);
    }

    private List<Candle> mapCandles(BitgetResponse<List<List<String>>> response) {

        if (!"00000".equals(response.code())) {
            throw new IllegalStateException("Bitget Response code " + response.code() + " - " + response.msg());
        }

        return response.data()
                .stream()
                .map(this::mapCandle)
                .toList();
    }


    private Candle mapCandle(List<String> data) {
        return new Candle(
                Instant.ofEpochMilli(Long.parseLong(data.get(0))),
                new BigDecimal(data.get(1)),
                new BigDecimal(data.get(2)),
                new BigDecimal(data.get(3)),
                new BigDecimal(data.get(4)),
                new BigDecimal(data.get(5)),
                new BigDecimal(data.get(6))
        );
    }



}
