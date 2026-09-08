package org.example;

import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConnectionTest {
    private final JdbcTemplate jdbcTemplate;

    public DatabaseConnectionTest(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void testConnection() {
        String version = jdbcTemplate.queryForObject(
                "SELECT version()",
                String.class
        );

        System.out.println("PostgreSQL connected: " + version);
    }
}
