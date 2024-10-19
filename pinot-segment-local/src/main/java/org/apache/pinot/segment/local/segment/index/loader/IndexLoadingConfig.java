/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.TimestampIndexUtils;


/**
 * Table level index loading config.
 */
public class IndexLoadingConfig {
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;
  public static final String READ_MODE_KEY = "readMode";

  private final InstanceDataManagerConfig _instanceDataManagerConfig;
  private final TableConfig _tableConfig;
  private final Schema _schema;

  // These fields can be modified after initialization
  // TODO: Revisit them
  private ReadMode _readMode = ReadMode.DEFAULT_MODE;
  private SegmentVersion _segmentVersion;
  private String _segmentTier;
  private Set<String> _knownColumns;
  private String _tableDataDir;
  private boolean _errorOnColumnBuildFailure;

  // Initialized by instance data manager config
  private String _instanceId;
  private boolean _isRealtimeOffHeapAllocation;
  private boolean _isDirectRealtimeOffHeapAllocation;
  private int _realtimeAvgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
  private String _segmentStoreURI;
  private String _segmentDirectoryLoader;
  private Map<String, Map<String, String>> _instanceTierConfigs;

  // Initialized by table config and schema
  private List<String> _sortedColumns = Collections.emptyList();
  private final Set<String> _invertedIndexColumns = new HashSet<>();
  private final Set<String> _rangeIndexColumns = new HashSet<>();
  private int _rangeIndexVersion = RangeIndexConfig.DEFAULT.getVersion();
  private final Set<String> _textIndexColumns = new HashSet<>();
  private final Set<String> _fstIndexColumns = new HashSet<>();
  private FSTType _fstIndexType = FSTType.LUCENE;
  private Map<String, JsonIndexConfig> _jsonIndexConfigs = new HashMap<>();
  private final Set<String> _noDictionaryColumns = new HashSet<>(); // TODO: replace this by _noDictionaryConfig.
  private final Map<String, String> _noDictionaryConfig = new HashMap<>();
  private final Set<String> _varLengthDictionaryColumns = new HashSet<>();
  private final Set<String> _onHeapDictionaryColumns = new HashSet<>();
  private final Map<String, BloomFilterConfig> _bloomFilterConfigs = new HashMap<>();
  private ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
  private boolean _enableDynamicStarTreeCreation;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs;
  private boolean _enableDefaultStarTree;
  private final Map<String, Map<String, String>> _columnProperties = new HashMap<>();
  private Map<String, FieldIndexConfigs> _indexConfigsByColName = new HashMap<>();

  private boolean _dirty = true;

  /**
   * NOTE: This step might modify the passed in table config and schema.
   *
   * TODO: Revisit the init handling. Currently it doesn't apply tiered config override
   */
  public IndexLoadingConfig(@Nullable InstanceDataManagerConfig instanceDataManagerConfig,
      @Nullable TableConfig tableConfig, @Nullable Schema schema) {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    _tableConfig = tableConfig;
    _schema = schema;
    init();
  }

  @VisibleForTesting
  public IndexLoadingConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig) {
    this(instanceDataManagerConfig, tableConfig, null);
  }

  @VisibleForTesting
  public IndexLoadingConfig(TableConfig tableConfig, @Nullable Schema schema) {
    this(null, tableConfig, schema);
  }

  /**
   * NOTE: Can be used in production code when we want to load a segment as is without any modifications.
   */
  public IndexLoadingConfig() {
    this(null, null, null);
  }

  @Nullable
  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  @Nullable
  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  @Nullable
  public Schema getSchema() {
    return _schema;
  }

  private void init() {
    if (_instanceDataManagerConfig != null) {
      extractFromInstanceConfig(_instanceDataManagerConfig);
    }
    if (_tableConfig != null) {
      extractFromTableConfigAndSchema(_tableConfig, _schema);
    }
  }

  private void extractFromInstanceConfig(InstanceDataManagerConfig instanceDataManagerConfig) {
    _instanceId = instanceDataManagerConfig.getInstanceId();

    ReadMode instanceReadMode = instanceDataManagerConfig.getReadMode();
    if (instanceReadMode != null) {
      _readMode = instanceReadMode;
    }

    String instanceSegmentVersion = instanceDataManagerConfig.getSegmentFormatVersion();
    if (instanceSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
    }

    _isRealtimeOffHeapAllocation = instanceDataManagerConfig.isRealtimeOffHeapAllocation();
    _isDirectRealtimeOffHeapAllocation = instanceDataManagerConfig.isDirectRealtimeOffHeapAllocation();

    String avgMultiValueCount = instanceDataManagerConfig.getAvgMultiValueCount();
    if (avgMultiValueCount != null) {
      _realtimeAvgMultiValueCount = Integer.parseInt(avgMultiValueCount);
    }
    _segmentStoreURI =
        instanceDataManagerConfig.getConfig().getProperty(CommonConstants.Server.CONFIG_OF_SEGMENT_STORE_URI);
    _segmentDirectoryLoader = instanceDataManagerConfig.getSegmentDirectoryLoader();

    Map<String, Map<String, String>> tierConfigs = instanceDataManagerConfig.getTierConfigs();
    _instanceTierConfigs = tierConfigs != null ? tierConfigs : Map.of();
  }

  private void extractFromTableConfigAndSchema(TableConfig tableConfig, @Nullable Schema schema) {
    if (schema != null) {
      TimestampIndexUtils.applyTimestampIndex(tableConfig, schema);
    }

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    String tableReadMode = indexingConfig.getLoadMode();
    if (tableReadMode != null) {
      _readMode = ReadMode.getEnum(tableReadMode);
    }

    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (sortedColumns != null) {
      _sortedColumns = sortedColumns;
    }

    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    if (invertedIndexColumns != null) {
      _invertedIndexColumns.addAll(invertedIndexColumns);
    }

    // Ignore jsonIndexColumns when jsonIndexConfigs is configured
    Map<String, JsonIndexConfig> jsonIndexConfigs = indexingConfig.getJsonIndexConfigs();
    if (jsonIndexConfigs != null) {
      _jsonIndexConfigs = jsonIndexConfigs;
    } else {
      List<String> jsonIndexColumns = indexingConfig.getJsonIndexColumns();
      if (jsonIndexColumns != null) {
        _jsonIndexConfigs = new HashMap<>();
        for (String jsonIndexColumn : jsonIndexColumns) {
          _jsonIndexConfigs.put(jsonIndexColumn, new JsonIndexConfig());
        }
      }
    }

    List<String> rangeIndexColumns = indexingConfig.getRangeIndexColumns();
    if (rangeIndexColumns != null) {
      _rangeIndexColumns.addAll(rangeIndexColumns);
    }

    _rangeIndexVersion = indexingConfig.getRangeIndexVersion();

    _fstIndexType = indexingConfig.getFSTIndexType();

    List<String> bloomFilterColumns = indexingConfig.getBloomFilterColumns();
    if (bloomFilterColumns != null) {
      for (String bloomFilterColumn : bloomFilterColumns) {
        _bloomFilterConfigs.put(bloomFilterColumn, new BloomFilterConfig(BloomFilterConfig.DEFAULT_FPP, 0, false));
      }
    }
    Map<String, BloomFilterConfig> bloomFilterConfigs = indexingConfig.getBloomFilterConfigs();
    if (bloomFilterConfigs != null) {
      _bloomFilterConfigs.putAll(bloomFilterConfigs);
    }

    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    if (noDictionaryColumns != null) {
      _noDictionaryColumns.addAll(noDictionaryColumns);
    }

    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        _columnProperties.put(fieldConfig.getName(), fieldConfig.getProperties());
      }
    }

    extractTextIndexColumnsFromTableConfig(tableConfig);
    extractFSTIndexColumnsFromTableConfig(tableConfig);

    Map<String, String> noDictionaryConfig = indexingConfig.getNoDictionaryConfig();
    if (noDictionaryConfig != null) {
      _noDictionaryConfig.putAll(noDictionaryConfig);
    }

    List<String> varLengthDictionaryColumns = indexingConfig.getVarLengthDictionaryColumns();
    if (varLengthDictionaryColumns != null) {
      _varLengthDictionaryColumns.addAll(varLengthDictionaryColumns);
    }

    List<String> onHeapDictionaryColumns = indexingConfig.getOnHeapDictionaryColumns();
    if (onHeapDictionaryColumns != null) {
      _onHeapDictionaryColumns.addAll(onHeapDictionaryColumns);
    }

    String tableSegmentVersion = indexingConfig.getSegmentFormatVersion();
    if (tableSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(tableSegmentVersion.toLowerCase());
    }

    String columnMinMaxValueGeneratorMode = indexingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode != null) {
      _columnMinMaxValueGeneratorMode =
          ColumnMinMaxValueGeneratorMode.valueOf(columnMinMaxValueGeneratorMode.toUpperCase());
    }

    refreshIndexConfigs();
  }

  public void refreshIndexConfigs() {
    TableConfig tableConfig = getTableConfigWithTierOverwrites();
    // Accessing the index configs for single-column index is handled by IndexType.getConfig() as defined in index-spi.
    // As the tableConfig is overwritten with tier specific configs, IndexType.getConfig() can access the tier
    // specific index configs transparently.
    _indexConfigsByColName = calculateIndexConfigsByColName(tableConfig, inferSchema());
    // Accessing the StarTree index configs is not handled by IndexType.getConfig(), so we manually update them.
    if (tableConfig != null) {
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      _enableDynamicStarTreeCreation = indexingConfig.isEnableDynamicStarTreeCreation();
      _starTreeIndexConfigs = indexingConfig.getStarTreeIndexConfigs();
      _enableDefaultStarTree = indexingConfig.isEnableDefaultStarTree();
    }
    _dirty = false;
  }

  /**
   * Calculates the map from column to {@link FieldIndexConfigs}, merging the information related to older configs (
   * which is also heavily used by tests) and the one included in the TableConfig (in case the latter is not null).
   *
   * This method does not modify the result of {@link #getFieldIndexConfigByColName()} or
   * {@link #getFieldIndexConfigByColName()}. To do so, call {@link #refreshIndexConfigs()}.
   *
   * The main difference between this method and
   * {@link FieldIndexConfigsUtil#createIndexConfigsByColName(TableConfig, Schema)} is that the former relays
   * on the TableConfig, while this method can be used even when the {@link IndexLoadingConfig} was configured by
   * calling the setter methods.
   */
  public Map<String, FieldIndexConfigs> calculateIndexConfigsByColName() {
    return calculateIndexConfigsByColName(getTableConfigWithTierOverwrites(), inferSchema());
  }

  private Map<String, FieldIndexConfigs> calculateIndexConfigsByColName(@Nullable TableConfig tableConfig,
      Schema schema) {
    return FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema, this::getDeserializer);
  }

  private <C extends IndexConfig> ColumnConfigDeserializer<C> getDeserializer(IndexType<C, ?, ?> indexType) {
    if (_tableConfig != null && _schema != null) {
      return indexType::getConfig;
    } else {
      return (tableConfig, schema) -> getAllKnownColumns().stream()
          .collect(Collectors.toMap(Function.identity(), col -> indexType.getDefaultConfig()));
    }
  }

  private TableConfig getTableConfigWithTierOverwrites() {
    return (_segmentTier == null || _tableConfig == null) ? _tableConfig
        : TableConfigUtils.overwriteTableConfigForTier(_tableConfig, _segmentTier);
  }

  private Schema inferSchema() {
    if (_schema != null) {
      return _schema;
    }
    Schema schema = new Schema();
    for (String column : getAllKnownColumns()) {
      schema.addField(new DimensionFieldSpec(column, FieldSpec.DataType.STRING, true));
    }
    return schema;
  }

  /**
   * Text index creation info for each column is specified
   * using {@link FieldConfig} model of indicating per column
   * encoding and indexing information. Since IndexLoadingConfig
   * is created from TableConfig, we extract the text index info
   * from fieldConfigList in TableConfig.
   * @param tableConfig table config
   */
  private void extractTextIndexColumnsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.TEXT)) {
          _textIndexColumns.add(fieldConfig.getName());
        }
      }
    }
  }

  private void extractFSTIndexColumnsFromTableConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      for (FieldConfig fieldConfig : fieldConfigList) {
        if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.FST)) {
          _fstIndexColumns.add(fieldConfig.getName());
        }
      }
    }
  }

  public ReadMode getReadMode() {
    return _readMode;
  }

  public void setReadMode(ReadMode readMode) {
    _readMode = readMode;
    _dirty = true;
  }

  public List<String> getSortedColumns() {
    return unmodifiable(_sortedColumns);
  }

  public Set<String> getInvertedIndexColumns() {
    return unmodifiable(_invertedIndexColumns);
  }

  public Set<String> getRangeIndexColumns() {
    return unmodifiable(_rangeIndexColumns);
  }

  public int getRangeIndexVersion() {
    return _rangeIndexVersion;
  }

  public FSTType getFSTIndexType() {
    return _fstIndexType;
  }

  /**
   * Used in two places:
   * (1) In {@link PhysicalColumnIndexContainer} to create the index loading info for immutable segments
   * (2) In RealtimeSegmentDataManager to create the RealtimeSegmentConfig.
   * RealtimeSegmentConfig is used to specify the text index column info for newly
   * to-be-created Mutable Segments
   * @return a set containing names of text index columns
   */
  public Set<String> getTextIndexColumns() {
    return unmodifiable(_textIndexColumns);
  }

  public Set<String> getFSTIndexColumns() {
    return unmodifiable(_fstIndexColumns);
  }

  public Map<String, JsonIndexConfig> getJsonIndexConfigs() {
    return unmodifiable(_jsonIndexConfigs);
  }

  public Map<String, Map<String, String>> getColumnProperties() {
    return unmodifiable(_columnProperties);
  }

  public Set<String> getNoDictionaryColumns() {
    return unmodifiable(_noDictionaryColumns);
  }

  public Map<String, String> getNoDictionaryConfig() {
    return unmodifiable(_noDictionaryConfig);
  }

  public Set<String> getVarLengthDictionaryColumns() {
    return unmodifiable(_varLengthDictionaryColumns);
  }

  public Set<String> getOnHeapDictionaryColumns() {
    return unmodifiable(_onHeapDictionaryColumns);
  }

  public Map<String, BloomFilterConfig> getBloomFilterConfigs() {
    return unmodifiable(_bloomFilterConfigs);
  }

  public boolean isEnableDynamicStarTreeCreation() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return _enableDynamicStarTreeCreation;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return unmodifiable(_starTreeIndexConfigs);
  }

  public boolean isEnableDefaultStarTree() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return _enableDefaultStarTree;
  }

  @Nullable
  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
  }

  /**
   * For tests only.
   */
  public void setSegmentVersion(SegmentVersion segmentVersion) {
    _segmentVersion = segmentVersion;
    _dirty = true;
  }

  public boolean isRealtimeOffHeapAllocation() {
    return _isRealtimeOffHeapAllocation;
  }

  public boolean isDirectRealtimeOffHeapAllocation() {
    return _isDirectRealtimeOffHeapAllocation;
  }

  public ColumnMinMaxValueGeneratorMode getColumnMinMaxValueGeneratorMode() {
    return _columnMinMaxValueGeneratorMode;
  }

  public String getSegmentStoreURI() {
    return _segmentStoreURI;
  }

  public int getRealtimeAvgMultiValueCount() {
    return _realtimeAvgMultiValueCount;
  }

  public String getSegmentDirectoryLoader() {
    return StringUtils.isNotBlank(_segmentDirectoryLoader) ? _segmentDirectoryLoader
        : SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME;
  }

  public PinotConfiguration getSegmentDirectoryConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(READ_MODE_KEY, _readMode);
    return new PinotConfiguration(props);
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getSegmentTier() {
    return _segmentTier;
  }

  public void setSegmentTier(String segmentTier) {
    _segmentTier = segmentTier;
    _dirty = true;
  }

  public String getTableDataDir() {
    return _tableDataDir;
  }

  public void setTableDataDir(String tableDataDir) {
    _tableDataDir = tableDataDir;
  }

  public boolean isErrorOnColumnBuildFailure() {
    return _errorOnColumnBuildFailure;
  }

  public void setErrorOnColumnBuildFailure(boolean errorOnColumnBuildFailure) {
    _errorOnColumnBuildFailure = errorOnColumnBuildFailure;
  }

  @Nullable
  public FieldIndexConfigs getFieldIndexConfig(String columnName) {
    if (_indexConfigsByColName == null || _dirty) {
      refreshIndexConfigs();
    }
    return _indexConfigsByColName.get(columnName);
  }

  public Map<String, FieldIndexConfigs> getFieldIndexConfigByColName() {
    if (_indexConfigsByColName == null || _dirty) {
      refreshIndexConfigs();
    }
    return unmodifiable(_indexConfigsByColName);
  }

  /**
   * Returns a subset of the columns on the table.
   *
   * When {@link #getSchema()} is defined, the subset is equal the columns on the schema. In other cases, this method
   * tries its bests to get the columns from other attributes like {@link #getTableConfig()}, which may also not be
   * defined or may not be complete.
   */
  public Set<String> getAllKnownColumns() {
    if (_schema != null) {
      return _schema.getColumnNames();
    }
    if (!_dirty && _knownColumns != null) {
      return _knownColumns;
    }
    if (_knownColumns == null) {
      _knownColumns = new HashSet<>();
    }
    if (_tableConfig != null) {
      List<FieldConfig> fieldConfigs = _tableConfig.getFieldConfigList();
      if (fieldConfigs != null) {
        for (FieldConfig fieldConfig : fieldConfigs) {
          _knownColumns.add(fieldConfig.getName());
        }
      }
    }
    _knownColumns.addAll(_columnProperties.keySet());
    _knownColumns.addAll(_invertedIndexColumns);
    _knownColumns.addAll(_fstIndexColumns);
    _knownColumns.addAll(_rangeIndexColumns);
    _knownColumns.addAll(_noDictionaryColumns);
    _knownColumns.addAll(_textIndexColumns);
    _knownColumns.addAll(_onHeapDictionaryColumns);
    _knownColumns.addAll(_varLengthDictionaryColumns);
    return _knownColumns;
  }

  public void setInstanceTierConfigs(Map<String, Map<String, String>> tierConfigs) {
    _instanceTierConfigs = new HashMap<>(tierConfigs);
    _dirty = true;
  }

  public Map<String, Map<String, String>> getInstanceTierConfigs() {
    return unmodifiable(_instanceTierConfigs);
  }

  private <E> List<E> unmodifiable(List<E> list) {
    return list == null ? null : Collections.unmodifiableList(list);
  }

  private <E> Set<E> unmodifiable(Set<E> set) {
    return set == null ? null : Collections.unmodifiableSet(set);
  }

  private <K, V> Map<K, V> unmodifiable(Map<K, V> map) {
    return map == null ? null : Collections.unmodifiableMap(map);
  }

  public void addKnownColumns(Set<String> columns) {
    if (_knownColumns == null) {
      _knownColumns = new HashSet<>(columns);
    } else {
      _knownColumns.addAll(columns);
    }
    _dirty = true;
  }
}
