package org.example;

import tools.jackson.databind.JsonNode;

public record ClassifiedMessage(
        MessageType type,
        JsonNode jsonTree
) {}
