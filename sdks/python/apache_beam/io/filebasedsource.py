#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A framework for developing sources for new file types.

To create a source for a new file type a sub-class of ``FileBasedSource`` should
be created. Sub-classes of ``FileBasedSource`` must implement the method
``FileBasedSource.read_records()``. Please read the documentation of that method
for more details.

For an example implementation of ``FileBasedSource`` see ``avroio.AvroSource``.
"""

from multiprocessing.pool import ThreadPool

from apache_beam.internal import pickler
from apache_beam.io import fileio
from apache_beam.io import iobase

import range_trackers

MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 25


class _ConcatSource(iobase.BoundedSource):
  """A ``BoundedSource`` that can group a set of ``BoundedSources``."""

  def __init__(self, sources):
    self._sources = sources

  @property
  def sources(self):
    return self._sources

  def estimate_size(self):
    return sum(s.estimate_size() for s in self._sources)

  def split(
      self, desired_bundle_size=None, start_position=None, stop_position=None):
    if start_position or stop_position:
      raise ValueError(
          'Multi-level initial splitting is not supported. Expected start and '
          'stop positions to be None. Received %r and %r respectively.',
          start_position, stop_position)

    for source in self._sources:
      # We assume all sub-sources to produce bundles that specify weight using
      # the same unit. For example, all sub-sources may specify the size in
      # bytes as their weight.
      for bundle in source.split(desired_bundle_size, None, None):
        yield bundle

  def get_range_tracker(self, start_position, stop_position):
    assert start_position is None
    assert stop_position is None
    # This will be invoked only when FileBasedSource is read without splitting.
    # For that case, we only support reading the whole source.
    return range_trackers.OffsetRangeTracker(0, len(self.sources))

  def read(self, range_tracker):
    for index, sub_source in enumerate(self.sources):
      if not range_tracker.try_claim(index):
        return

      sub_source_tracker = sub_source.get_range_tracker(None, None)
      for record in sub_source.read(sub_source_tracker):
        yield record

  def default_output_coder(self):
    if self._sources:
      # Getting coder from the first sub-sources. This assumes all sub-sources
      # to produce the same coder.
      return self._sources[0].default_output_coder()
    else:
      # Defaulting to PickleCoder.
      return super(_ConcatSource, self).default_output_coder()


class FileBasedSource(iobase.BoundedSource):
  """A ``BoundedSource`` for reading a file glob of a given type."""

  def __init__(self,
               file_pattern,
               min_bundle_size=0,
               # TODO(BEAM-614)
               compression_type=fileio.CompressionTypes.UNCOMPRESSED,
               splittable=True):
    """Initializes ``FileBasedSource``.

    Args:
      file_pattern: the file glob to read.
      min_bundle_size: minimum size of bundles that should be generated when
                       performing initial splitting on this source.
      compression_type: compression type to use
      splittable: whether FileBasedSource should try to logically split a single
                  file into data ranges so that different parts of the same file
                  can be read in parallel. If set to False, FileBasedSource will
                  prevent both initial and dynamic splitting of sources for
                  single files. File patterns that represent multiple files may
                  still get split into sources for individual files. Even if set
                  to True by the user, FileBasedSource may choose to not split
                  the file, for example, for compressed files where currently
                  it is not possible to efficiently read a data range without
                  decompressing the whole file.
    Raises:
      TypeError: when compression_type is not valid or if file_pattern is not a
                 string.
      ValueError: when compression and splittable files are specified.
    """
    if not isinstance(file_pattern, basestring):
      raise TypeError(
          '%s: file_pattern must be a string;  got %r instead' %
          (self.__class__.__name__, file_pattern))

    if compression_type == fileio.CompressionTypes.AUTO:
      raise ValueError('FileBasedSource currently does not support '
                       'CompressionTypes.AUTO. Please explicitly specify the '
                       'compression type or use '
                       'CompressionTypes.UNCOMPRESSED if file is '
                       'uncompressed.')

    self._pattern = file_pattern
    self._concat_source = None
    self._min_bundle_size = min_bundle_size
    if not fileio.CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    self._compression_type = compression_type
    if compression_type != fileio.CompressionTypes.UNCOMPRESSED:
      # We can't split compressed files efficiently so turn off splitting.
      self._splittable = False
    else:
      self._splittable = splittable

  def _get_concat_source(self):
    if self._concat_source is None:
      single_file_sources = []
      file_names = [f for f in fileio.ChannelFactory.glob(self._pattern)]
      sizes = FileBasedSource._estimate_sizes_in_parallel(file_names)

      for index, file_name in enumerate(file_names):
        if sizes[index] == 0:
          continue  # Ignoring empty file.

        single_file_source = _SingleFileSource(
            self, file_name,
            0,
            sizes[index],
            min_bundle_size=self._min_bundle_size)
        single_file_sources.append(single_file_source)
      self._concat_source = _ConcatSource(single_file_sources)
    return self._concat_source

  def open_file(self, file_name):
    return fileio.ChannelFactory.open(
        file_name, 'rb', 'application/octet-stream',
        compression_type=self._compression_type)

  @staticmethod
  def _estimate_sizes_in_parallel(file_names):

    def _calculate_size_of_file(file_name):
      return fileio.ChannelFactory.size_in_bytes(file_name)

    return ThreadPool(MAX_NUM_THREADS_FOR_SIZE_ESTIMATION).map(
        _calculate_size_of_file, file_names)

  def split(
      self, desired_bundle_size=None, start_position=None, stop_position=None):
    return self._get_concat_source().split(
        desired_bundle_size=desired_bundle_size,
        start_position=start_position,
        stop_position=stop_position)

  def estimate_size(self):
    return self._get_concat_source().estimate_size()

  def read(self, range_tracker):
    return self._get_concat_source().read(range_tracker)

  def get_range_tracker(self, start_position, stop_position):
    return self._get_concat_source().get_range_tracker(start_position,
                                                       stop_position)

  def default_output_coder(self):
    return self._get_concat_source().default_output_coder()

  def read_records(self, file_name, offset_range_tracker):
    """Returns a generator of records created by reading file 'file_name'.

    Args:
      file_name: a ``string`` that gives the name of the file to be read. Method
                 ``FileBasedSource.open_file()`` must be used to open the file
                 and create a seekable file object.
      offset_range_tracker: a object of type ``OffsetRangeTracker``. This
                            defines the byte range of the file that should be
                            read. See documentation in
                            ``iobase.BoundedSource.read()`` for more information
                            on reading records while complying to the range
                            defined by a given ``RangeTracker``.

    Returns:
      a iterator that gives the records read from the given file.
    """
    raise NotImplementedError

  @property
  def splittable(self):
    return self._splittable

  @staticmethod
  def split_points_remaining_helper(
      stop_position, current_position, done_reading):
    """A utility function for implementing a remaining splits points callback.

    This helper function can be used to develop a remaining split points
    callback, which can be set to a RangeTracker using method
    ``RangeTracker.set_split_points_remaining_callback()``.

    For example uses of this function see:
     * ``filebasedsource_test.LineSource``
     * ``avroio.AvroSource``

    Developing a remaining split points callback using this function will be
    better than not setting a callback at all. Latter will result in
    ``RangeTracker.split_points()`` function always returning
    ``RangeTracker.SPLIT_POINTS_UNKNOWN`` for the number of remaining split
    points.

    If this function returns ``iobase.RangeTracker.SPLIT_POINTS_UNKNOWN``,
    callers of this function may use other means to exactly determine the number
    of remaining split points.

    This was developed as a static helper instead of a method in
    ``FileBasedSource`` since ``FileBasedSource`` is not aware of the current
    state of the source read operation.

    Args:
      stop_position: stop position of the range of positions to be read by the
                     source.
      current_position: position of the current record being read by the source.
                        Should be negative if no records have been read.
      done_reading: ``True``, if source has finished reading all the records
      that belong to the range of positions assigned to the source. ``False``
      otherwise.
    Returns:
      number of split points remaining. Returns
      ``iobase.RangeTracker.SPLIT_POINTS_UNKNOWN`` if number of split points
      remaining cannot be determined.
    """

    if done_reading:
      # We have finished reading. All split points have been consumed.
      return 0
    elif current_position >= 0 and current_position + 1 >= stop_position:
      # Next element is outside the range. Since done_reading is not set we are
      # at the last split point.
      return 1
    else:
      # Number of remaining split points cannot be determined with the provided
      # information.
      return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN


class _SingleFileSource(iobase.BoundedSource):
  """Denotes a source for a specific file type.

  This should be sub-classed to add support for reading a new file type.
  """

  def __init__(self, file_based_source, file_name, start_offset, stop_offset,
               min_bundle_size=0):
    if not (isinstance(start_offset, int) or isinstance(start_offset, long)):
      raise ValueError(
          'start_offset must be a number. Received: %r', start_offset)
    if stop_offset != range_trackers.OffsetRangeTracker.OFFSET_INFINITY:
      if not (isinstance(stop_offset, int) or isinstance(stop_offset, long)):
        raise ValueError(
            'stop_offset must be a number. Received: %r', stop_offset)
      if start_offset >= stop_offset:
        raise ValueError(
            'start_offset must be smaller than stop_offset. Received %d and %d '
            'for start and stop offsets respectively',
            start_offset, stop_offset)

    self._file_name = file_name
    self._is_gcs_file = file_name.startswith('gs://') if file_name else False
    self._start_offset = start_offset
    self._stop_offset = stop_offset
    self._min_bundle_size = min_bundle_size
    self._file_based_source = file_based_source

  def split(self, desired_bundle_size, start_offset=None, stop_offset=None):
    if start_offset is None:
      start_offset = self._start_offset
    if stop_offset is None:
      stop_offset = self._stop_offset

    if self._file_based_source.splittable:
      bundle_size = max(desired_bundle_size, self._min_bundle_size)

      bundle_start = start_offset
      while bundle_start < stop_offset:
        bundle_stop = min(bundle_start + bundle_size, stop_offset)
        yield iobase.SourceBundle(
            bundle_stop - bundle_start,
            _SingleFileSource(
                # Copying this so that each sub-source gets a fresh instance.
                pickler.loads(pickler.dumps(self._file_based_source)),
                self._file_name,
                bundle_start,
                bundle_stop,
                min_bundle_size=self._min_bundle_size),
            bundle_start,
            bundle_stop)
        bundle_start = bundle_stop
    else:
      # Returning a single sub-source with end offset set to OFFSET_INFINITY (so
      # that all data of the source gets read) since this source is
      # unsplittable. Choosing size of the file as end offset will be wrong for
      # certain unsplittable source, e.g., compressed sources.
      yield iobase.SourceBundle(
          stop_offset - start_offset,
          _SingleFileSource(
              self._file_based_source,
              self._file_name,
              start_offset,
              range_trackers.OffsetRangeTracker.OFFSET_INFINITY,
              min_bundle_size=self._min_bundle_size
          ),
          start_offset,
          range_trackers.OffsetRangeTracker.OFFSET_INFINITY
      )

  def estimate_size(self):
    return self._stop_offset - self._start_offset

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = self._start_offset
    if stop_position is None:
      # If file is unsplittable we choose OFFSET_INFINITY as the default end
      # offset so that all data of the source gets read. Choosing size of the
      # file as end offset will be wrong for certain unsplittable source, for
      # e.g., compressed sources.
      stop_position = (
          self._stop_offset if self._file_based_source.splittable
          else range_trackers.OffsetRangeTracker.OFFSET_INFINITY)

    range_tracker = range_trackers.OffsetRangeTracker(
        start_position, stop_position)
    if not self._file_based_source.splittable:
      range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

    return range_tracker

  def read(self, range_tracker):
    return self._file_based_source.read_records(self._file_name, range_tracker)
