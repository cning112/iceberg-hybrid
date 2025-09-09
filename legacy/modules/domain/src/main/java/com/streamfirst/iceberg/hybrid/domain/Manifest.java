package com.streamfirst.iceberg.hybrid.domain;
import java.util.List;
public record Manifest(String path, List<FileRef> files) {}
