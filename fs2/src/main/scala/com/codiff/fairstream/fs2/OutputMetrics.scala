/*
 * Copyright 2026 codiff
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.codiff.fairstream
package fs2

import cats.effect.Ref
import cats.effect.Sync
import cats.syntax.all._

import _root_.fs2.Stream

final class OutputMetrics[F[_]: cats.Monad] private (
    counts: Ref[F, Map[String, Long]]
) {
  def snapshot: F[Map[String, Long]] = counts.get

  def count(tag: String): F[Long] = counts.get.map(_.getOrElse(tag, 0L))

  def track[A](tag: String)(stream: Stream[F, A]): Stream[F, A] =
    stream.evalTap(_ => counts.update(current =>
      current.updated(tag, current.getOrElse(tag, 0L) + 1L)
    ))
}

object OutputMetrics {
  def create[F[_]: Sync]: F[OutputMetrics[F]] =
    Ref.of[F, Map[String, Long]](Map.empty).map(new OutputMetrics(_))
}