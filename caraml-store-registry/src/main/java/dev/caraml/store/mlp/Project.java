package dev.caraml.store.mlp;

import java.util.List;

public record Project(String name, String stream, String team, List<Label> labels) {}

