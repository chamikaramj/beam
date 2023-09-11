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
package org.apache.beam.examples;

import org.apache.beam.runners.core.construction.ExternalTranslationOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CreateReplace {

  static void createReplace(PipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    PCollection<Integer> result =
      p.apply(Create.of(1, 2, 3))
          .apply(
              MapElements.into(TypeDescriptors.integers())
                  .via(value -> value * 2));
    PAssert.that(result).containsInAnyOrder(2, 4, 6);
    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ExternalTranslationOptions.class);
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    createReplace(options);
  }
}
