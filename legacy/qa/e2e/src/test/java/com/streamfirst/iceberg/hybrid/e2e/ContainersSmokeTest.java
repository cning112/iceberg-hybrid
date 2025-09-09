package com.streamfirst.iceberg.hybrid.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("containers")
public class ContainersSmokeTest {

  @Test
  void minio_starts() {
    try (MinIOContainer minio = new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-09-22T00-33-43Z"))) {
      minio.start();
      assertThat(minio.isRunning()).isTrue();
    }
  }
}
