/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.core.construction;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utility methods for translating a {@link Impulse} to and from {@link RunnerApi} representations.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ImpulseTranslation {
  private static class ImpulseTranslator implements TransformPayloadTranslator<Impulse> {
    @Override
    public String getUrn(Impulse transform) {
      return getUrn();
    }

    @Override
    public String getUrn() {
      return PTransformTranslation.IMPULSE_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Impulse> application, SdkComponents components) throws IOException {
      return FunctionSpec.newBuilder().setUrn(getUrn(application.getTransform())).build();
    }

    @Override
    public @Nullable Row toConfigRow(Impulse transform) {
      Impulse impulse = (Impulse) transform;
      System.out.println("Found impulse transform: " + impulse);
      Schema schema = Schema.builder().build();
      return Row.nullRow(schema);
    }

    @Override
    public @Nullable Impulse fromConfigRow(Row configRow) {
      if (configRow.getFieldCount() != 0) {
        throw new IllegalArgumentException("Received an unexpected construction Row for impulse");
      }
      return Impulse.create();
    }
  }

  /** Registers {@link ImpulseTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(Impulse.class, new ImpulseTranslator());
    }
  }
}
