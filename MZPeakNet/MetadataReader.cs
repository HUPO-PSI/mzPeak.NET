using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;
using MZPeak.Compute;
using MZPeak.Reader.Visitors;
using Microsoft.Extensions.Logging;
using System.Numerics;
using MZPeak.Storage;
using ParquetSharp.Arrow;
using System.Security.Cryptography.X509Certificates;


namespace MZPeak.Metadata;


using SpacingModels = Dictionary<ulong, SpacingInterpolationModel<double>>;
using NativeIdIndex = Dictionary<ulong, string?>;


/// <summary>
/// A base class for generic metadata table reading
/// </summary>
public abstract class MetadataReaderBase<T>
{
    internal static ILogger? Logger = null;

    protected MzPeakMetadata mzPeakMetadata;

    protected MzPeakFacetNamespace Namespace;

    protected Dictionary<ulong, ulong?> DataPointCounts { get; set; }
    protected Dictionary<ulong, ulong?> PeakCounts { get; set; }

    /// <summary>Gets the file description metadata.</summary>
    public FileDescription FileDescription => mzPeakMetadata.FileDescription;
    /// <summary>Gets the list of instrument configurations.</summary>
    public List<InstrumentConfiguration> InstrumentConfigurations => mzPeakMetadata.InstrumentConfigurations;
    /// <summary>Gets the list of software used.</summary>
    public List<Software> Softwares => mzPeakMetadata.Softwares;
    /// <summary>Gets the list of samples.</summary>
    public List<Sample> Samples => mzPeakMetadata.Samples;
    /// <summary>Gets the list of data processing methods.</summary>
    public List<DataProcessingMethod> DataProcessingMethods => mzPeakMetadata.DataProcessingMethods;
    /// <summary>Gets the run-level metadata.</summary>
    public MSRun Run => mzPeakMetadata.Run;

    protected MetadataReaderBase(MzPeakMetadata mzPeakMetadata, MzPeakFacetNamespace parquetNamespace)
    {
        this.mzPeakMetadata = mzPeakMetadata;
        Namespace = parquetNamespace;
        DataPointCounts = new();
        PeakCounts = new();
    }

    protected async Task<ChunkedArray?> ReadAllRowsOf(FileReader fileReader)
    {
        List<IArrowArray> members = [];
        if (fileReader == null) return null;
        var reader = fileReader.GetRecordBatchReader();
        int ctr = 0;
        while (true)
        {
            RecordBatch batch = await reader.ReadNextRecordBatchAsync();
            if (batch == null)
            {
                Logger?.LogDebug($"Read {ctr} batches from {this}");
                break;
            }
            Logger?.LogDebug("batch {ctr}, {batch.Length} items", batch, ctr);
            ctr++;
            members.Add(batch.AsStructArry());
        }
        return members.Count > 0 ? new ChunkedArray(members) : null;
    }

    protected void GetNativeIdsFrom(StructArray? table, ref NativeIdIndex nativeIds)
    {
        if (table == null)
        {
            return;
        }

        var dtype = (StructType)table.Data.DataType;
        var fieldIdx = dtype.GetFieldIndex("id");
        if (fieldIdx < 0)
        {
            return;
        }

        var indexArr = (UInt64Array)table.Fields[0];
        var modelArr = (LargeStringArray)table.Fields[fieldIdx];
        nativeIds.EnsureCapacity(indexArr.Length);
        for (var i = 0; i < indexArr.Length; i++)
        {
            var index = indexArr.GetValue(i);
            if (index == null)
            {
                continue;
            }
            var nativeId = modelArr.GetString(i);
            nativeIds.Add((ulong)index, nativeId);
        }
    }

    /// <summary>Gets the number of entries in the metadata table.</summary>
    public abstract long Length { get; }

    /// <summary>Loads all metadata entries into a list.</summary>
    public abstract List<T> BulkLoad();

    /// <summary>Gets a single metadata entry by index.</summary>
    /// <param name="index">The entry index.</param>
    public abstract T Get(ulong index);

    protected void loadEntryCounts<U>(PrimitiveArray<U> countArray, UInt64Array indexArr, Dictionary<ulong, ulong?> accumulator) where U : struct, INumber<U>
    {
        foreach (var (i, c) in indexArr.AsEnumerable().Zip(countArray.AsEnumerable()))
        {
            if (i == null) continue;
            if (c == null)
            {
                accumulator[(ulong)i] = null;
            }
            else
            {
                var count = (U)c;
                accumulator[(ulong)i] = ulong.CreateSaturating(count);
            }
        }
    }

    protected bool loadCountFrom(ChunkedArray mainTable, SpectrumProperties column, Dictionary<ulong, ulong?> accumulator)
    {
        var cols = Namespace.ColumnMappings(DataKind.Metadata);
        var query = cols?.Find(c => c.Accession == column.CURIE());
        if (query == null) return false;
        int? countCol = null;
        for (var i = 0; i < mainTable.ArrayCount; i++)
        {
            var chunk = (StructArray)mainTable.Array(i);
            var idxField = (UInt64Array)chunk.Fields[0];
            if (countCol == null)
            {
                countCol = ((StructType)chunk.Data.DataType).GetFieldIndex(query.Path.Last());
                if (countCol < 0)
                {
                    countCol = null;
                }
            }
            if (countCol == null) {
                return false;
            };
            var countField = chunk.Fields[(int)countCol];
            switch (countField.Data.DataType.TypeId)
            {
                case ArrowTypeId.UInt32:
                    {
                        loadEntryCounts((UInt32Array)countField, idxField, accumulator);
                        break;
                    }
                case ArrowTypeId.UInt64:
                    {
                        loadEntryCounts((UInt64Array)countField, idxField, accumulator);
                        break;
                    }
                case ArrowTypeId.Int32:
                    {
                        loadEntryCounts((Int32Array)countField, idxField, accumulator);
                        break;
                    }
                case ArrowTypeId.Int64:
                    {
                        loadEntryCounts((Int64Array)countField, idxField, accumulator);
                        break;
                    }
                default:
                    {
                        throw new InvalidOperationException($"Unsupported {query} type {countField.Data.DataType}");
                    }
            }
        }
        return true;
    }

    virtual protected ChunkedArray? MainTable => null;

    /// <summary>
    /// Get the number of profile data points recorded being stored for the requested index
    /// </summary>
    /// <param name="index">The entry index to look up data point counts for</param>
    /// <returns>The number of profile data points, or <c>null</c> if no points were stored</returns>
    public ulong? NumberOfDataPointsFor(ulong index)
    {
        if (DataPointCounts.Count == 0)
        {
            if (MainTable == null) return null;
            loadCountFrom(MainTable, SpectrumProperties.NumberOfDataPoints, DataPointCounts);
        }
        ulong? outVal;
        DataPointCounts.TryGetValue(index, out outVal);
        return outVal;
    }

    /// <summary>
    /// Get the number of discrete peaks recorded being stored for the requested index
    /// </summary>
    /// <param name="index">The entry index to look up peak counts for</param>
    /// <returns>The number of discrete peaks, or <c>null</c> if no peaks were stored</returns>
    public ulong? NumberOfPeaks(ulong index)
    {
        if (PeakCounts.Count == 0)
        {
            if (MainTable == null) return null;
            loadCountFrom(MainTable, SpectrumProperties.NumberOfPeaks, PeakCounts);
        }
        ulong? outVal;
        PeakCounts.TryGetValue(index, out outVal);
        return outVal;
    }

    /// <summary>Gets native IDs keyed by entry index.</summary>
    public NativeIdIndex GetNativeIds()
    {
        var tab = new NativeIdIndex();
        if (MainTable == null)
        {
            return tab;
        }
        for (var i = 0; i < MainTable.ArrayCount; i++)
        {
            var chunk = MainTable.Array(i);
            GetNativeIdsFrom((StructArray)chunk, ref tab);
        }
        return tab;
    }
}


/// <summary>
/// Reader for spectrum metadata from Parquet files.
/// </summary>
public class SpectrumMetadataReader : MetadataReaderBase<SpectrumDescription>
{
    ChunkedArray? spectrumMetadata = null;
    ChunkedArray? scanMetadata = null;
    ChunkedArray? precursorMetadata = null;
    ChunkedArray? selectedIonMetadata = null;

    /// <summary>Gets the number of spectra.</summary>
    public override long Length
    {
        get
        {
            if (SpectrumMetadata == null)
            {
                InitializeTables().Wait();
            }
            return SpectrumMetadata == null ? 0 : SpectrumMetadata.Length;
        }
    }

    /// <summary>Creates a spectrum metadata reader.</summary>
    /// <param name="fileReader">The Parquet file reader.</param>
    /// <param name="initializeFacets">Whether to initialize tables immediately.</param>
    public SpectrumMetadataReader(MzPeakFacetNamespace parquetNamespace, bool initializeFacets = true) : base(MzPeakMetadata.FromFileIndex(parquetNamespace.FileIndex), parquetNamespace)
    {
        if (initializeFacets)
        {
            InitializeTables().Wait();
        }
    }

    /// <summary>
    /// Get the min and max m/z values across all spectra.
    ///
    /// This depends upon the MS:1000527 and MS:1000528 parameters, assuming they are present and are mapped to columns.
    /// </summary>
    /// <returns></returns>
    public (double, double)? MZRange()
    {
        var handle = Namespace.OpenMetadata();
        var mapping = Namespace.ColumnMappings(DataKind.Metadata);
        if (mapping == null || handle == null) return null;
        int lowColIdx = mapping.FindIndex(c => c.Accession == "MS:1000528");
        int hiColIdx = mapping.FindIndex(c =>  c.Accession == "MS:1000527");
        if (lowColIdx == -1 || hiColIdx == -1) return null;
        var lowCol = mapping[lowColIdx];
        var hiCol = mapping[hiColIdx];

        lowColIdx = -1;
        hiColIdx = -1;
        var minValue = double.PositiveInfinity;
        var maxValue = double.NegativeInfinity;
        for (var i = 0; i < handle.NumRowGroups; i++)
        {
            var rg = handle.ParquetReader.RowGroup(i);
            var rgMeta = rg.MetaData;
            if (lowColIdx == -1)
            {
                var q = string.Join('.', lowCol.Path);
                for (var j = 0; j < rgMeta.NumColumns; j++)
                {
                    var col = handle.ParquetReader.FileMetaData.Schema.Column(j);
                    if (col.Path.ToDotString() == q)
                    {
                        lowColIdx = j;
                        break;
                    }
                }
            }
            if (hiColIdx == -1)
            {
                var q = string.Join('.', hiCol.Path);
                for (var j = 0; j < rgMeta.NumColumns; j++)
                {
                    var col = handle.ParquetReader.FileMetaData.Schema.Column(j);
                    if (col.Path.ToDotString() == q)
                    {
                        hiColIdx = j;
                        break;
                    }
                }
            }
            if (hiColIdx == -1 || lowColIdx == -1) return null;

            var lowColMeta = rgMeta.GetColumnChunkMetaData(lowColIdx);
            if (!lowColMeta.IsStatsSet) continue;
            if (!(lowColMeta.Statistics?.HasMinMax ?? false)) continue;

            var hiColMeta = rgMeta.GetColumnChunkMetaData(hiColIdx);
            if (!hiColMeta.IsStatsSet) continue;
            if (!(hiColMeta.Statistics?.HasMinMax ?? false)) continue;

            var minValueOf = Convert.ToDouble(lowColMeta.Statistics.MaxUntyped);
            var maxValueOf = Convert.ToDouble(hiColMeta.Statistics.MaxUntyped);
            minValue = double.Min(minValue, minValueOf);
            maxValue = double.Max(maxValue, maxValueOf);
        }
        return (minValue, maxValue);
    }

    void loadSpectrumInterpolationModels(ListArray modelArr, UInt64Array indexArr, ref SpacingModels accumulator)
    {
        for (var i = 0; i < indexArr.Length; i++)
        {
            var index = indexArr.GetValue(i);
            if (index == null)
            {
                continue;
            }
            if (modelArr.IsNull(i))
            {
                continue;
            }
            var modelAt = modelArr.GetSlicedValues(i);
            var coefs = SpacingInterpolationModel<double>.FromArray(modelAt);
            if (coefs != null)
            {
                accumulator[(ulong)index] = coefs;
            }
        }
    }

    void loadSpectrumInterpolationModels(LargeListArray modelArr, UInt64Array indexArr, ref SpacingModels accumulator)
    {
        for (var i = 0; i < indexArr.Length; i++)
        {
            var index = indexArr.GetValue(i);
            if (index == null)
            {
                continue;
            }
            if (modelArr.IsNull(i) || modelArr.GetValueLength(i) == 0)
            {
                continue;
            }
            var modelAt = modelArr.GetSlicedValues(i);
            var coefs = SpacingInterpolationModel<double>.FromArray(modelAt);
            if (coefs != null)
            {
                accumulator[(ulong)index] = coefs;
            }
        }
    }

    /// <summary>Gets spacing interpolation models keyed by spectrum index.</summary>
    public SpacingModels GetSpacingModelIndex()
    {
        SpacingModels acc = new();
        if (SpectrumMetadata == null)
        {
            return acc;
        }

        if (SpectrumMetadata.ArrayCount == 0)
        {
            return acc;
        }

        var dtype = (StructType)SpectrumMetadata.Array(0).Data.DataType;
        var fieldIdx = dtype.GetFieldIndex("mz_delta_model");

        if (fieldIdx < 0)
        {
            return new();
        }

        for (var i = 0; i < SpectrumMetadata.ArrayCount; i++)
        {
            var chunk = (StructArray)SpectrumMetadata.Array(i);
            var indexArr = (UInt64Array)chunk.Fields[0];
            var modelArr = chunk.Fields[fieldIdx];
            if (modelArr.Data.DataType.TypeId == ArrowTypeId.List)
            {
                loadSpectrumInterpolationModels((ListArray)modelArr, indexArr, ref acc);
            }
            else if (modelArr.Data.DataType.TypeId == ArrowTypeId.LargeList)
            {
                loadSpectrumInterpolationModels((LargeListArray)modelArr, indexArr, ref acc);
            }
            else
            {
                throw new NotImplementedException($"{modelArr.Data.DataType.Name} not supported");
            }
        }
        return acc;
    }

    /// <summary>Gets or sets the spectrum metadata table.</summary>
    public ChunkedArray? SpectrumMetadata
    {
        get
        {
            if (spectrumMetadata == null)
            {
                InitializeTables().Wait();
            }
            return spectrumMetadata;
        }
        set => spectrumMetadata = value;
    }

    /// <summary>Gets or sets the scan metadata table.</summary>
    public ChunkedArray? ScanMetadata
    {
        get
        {
            if (scanMetadata == null)
            {
                InitializeTables().Wait();
            }
            return scanMetadata;
        }
        set => scanMetadata = value;
    }

    /// <summary>Gets or sets the precursor metadata table.</summary>
    public ChunkedArray? PrecursorMetadata
    {
        get
        {
            if (precursorMetadata == null)
            {
                InitializeTables().Wait();
            }
            return precursorMetadata;
        }
        set => precursorMetadata = value;
    }

    /// <summary>Gets or sets the selected ion metadata table.</summary>
    public ChunkedArray? SelectedIonMetadata
    {
        get
        {
            if (selectedIonMetadata == null)
            {
                InitializeTables().Wait();
            }
            return selectedIonMetadata;
        }
        set => selectedIonMetadata = value;
    }

    /// <summary>Loads all spectrum descriptions.</summary>
    public override List<SpectrumDescription> BulkLoad()
    {
        if (SpectrumMetadata == null) return new();
        var spectra = new List<SpectrumInfo>();
        for (var i = 0; i < SpectrumMetadata.ArrayCount; i++)
        {
            var vis = new SpectrumVisitor(Namespace.FindEntry(DataKind.Metadata)?.ColumnMappings);
            vis.Visit(SpectrumMetadata.Array(i));
            spectra.AddRange(vis.Values);
        }
        var descrs = spectra.Select(s => new SpectrumDescription(s, new(), new(), new())).ToList();
        if (ScanMetadata != null)
        {
            for (var i = 0; i < ScanMetadata.ArrayCount; i++)
            {
                var vis = new ScanVisitor(Namespace.FindEntry(DataKind.Scans)?.ColumnMappings);
                vis.Visit(ScanMetadata.Array(i));
                foreach (var rec in vis.Values)
                {
                    descrs[(int)rec.SourceIndex].Scans.Add(rec);
                }
            }
        }
        if (PrecursorMetadata != null)
        {
            for (var i = 0; i < PrecursorMetadata.ArrayCount; i++)
            {
                var vis = new PrecursorVisitor(Namespace.FindEntry(DataKind.Precursors)?.ColumnMappings);
                vis.Visit(PrecursorMetadata.Array(i));
                foreach (var rec in vis.Values)
                {
                    descrs[(int)rec.SourceIndex].Precursors.Add(rec);
                }
            }
        }
        if (SelectedIonMetadata != null)
        {
            for (var i = 0; i < SelectedIonMetadata.ArrayCount; i++)
            {
                var vis = new SelectedIonVisitor(Namespace.FindEntry(DataKind.SelectedIons)?.ColumnMappings);
                vis.Visit(SelectedIonMetadata.Array(i));
                foreach (var rec in vis.Values)
                {
                    descrs[(int)rec.SourceIndex].SelectedIons.Add(rec);
                }
            }
        }
        return descrs;
    }

    protected override ChunkedArray? MainTable => SpectrumMetadata;

    SpectrumDescription GetSpectrum(ulong index)
    {
        if (SpectrumMetadata == null) throw new IndexOutOfRangeException($"{index} out of spectrum index range");
        UInt64Array idxArr;
        SpectrumInfo? rec = null;
        for (var i = 0; i < SpectrumMetadata.ArrayCount; i++)
        {
            var chunk = (StructArray)SpectrumMetadata.Array(i);
            idxArr = (UInt64Array)chunk.Fields[0];
            var first = Compute.Compute.FirstNotNull(idxArr);
            var last = Compute.Compute.LastNotNull(idxArr);
            if (last == null || first == null || first.Value.Item1 > index || last.Value.Item1 < index) continue;
            var mask = Compute.Compute.Equal(idxArr, index);
            var recs = Compute.Compute.Filter(chunk, mask);
            var visitor = new SpectrumVisitor(Namespace.FindEntry(DataKind.Metadata)?.ColumnMappings);
            visitor.Visit(recs);
            rec = visitor.Values[0];
            break;
        }
        if (rec == null) throw new IndexOutOfRangeException($"{index} out of spectrum index range");

        var pn = rec.Parameters.Find(p => p.AccessionCURIE == "MS:1000127");
        List<ScanInfo> scanRecs = new();
        if (ScanMetadata != null)
        {
            for (var i = 0; i < ScanMetadata.ArrayCount; i++)
            {
                var chunk = (StructArray)ScanMetadata.Array(i);
                idxArr = (UInt64Array)chunk.Fields[0];
                var first = Compute.Compute.FirstNotNull(idxArr);
                var last = Compute.Compute.LastNotNull(idxArr);
                if (last == null || first == null || first.Value.Item1 > index || last.Value.Item1 < index) continue;
                var mask = Compute.Compute.Equal(idxArr, index);
                var recs = Compute.Compute.Filter(chunk, mask);
                var visitor = new ScanVisitor(Namespace.FindEntry(DataKind.Scans)?.ColumnMappings);
                visitor.Visit(recs);
                scanRecs.AddRange(visitor.Values);
                break;
            }
        }
        List<PrecursorInfo> precursorInfos = new();
        if (PrecursorMetadata != null)
        {
            for (var i = 0; i < PrecursorMetadata.ArrayCount; i++)
            {
                var chunk = (StructArray)PrecursorMetadata.Array(i);
                idxArr = (UInt64Array)chunk.Fields[0];
                var first = Compute.Compute.FirstNotNull(idxArr);
                var last = Compute.Compute.LastNotNull(idxArr);
                if (last == null || first == null || first.Value.Item1 > index || last.Value.Item1 < index) continue;
                var mask = Compute.Compute.Equal(idxArr, index);
                var recs = Compute.Compute.Filter(chunk, mask);
                var visitor = new PrecursorVisitor(Namespace.FindEntry(DataKind.Precursors)?.ColumnMappings);
                visitor.Visit(recs);
                precursorInfos.AddRange(visitor.Values);
                break;
            }
        }
        List<SelectedIonInfo> selectedIons = new();
        if (SelectedIonMetadata != null)
        {
            for (var i = 0; i < SelectedIonMetadata.ArrayCount; i++)
            {
                var chunk = (StructArray)SelectedIonMetadata.Array(i);
                idxArr = (UInt64Array)chunk.Fields[0];
                var first = Compute.Compute.FirstNotNull(idxArr);
                var last = Compute.Compute.LastNotNull(idxArr);
                if (last == null || first == null || first.Value.Item1 > index || last.Value.Item1 < index) continue;
                var mask = Compute.Compute.Equal(idxArr, index);
                var recs = Compute.Compute.Filter(chunk, mask);
                var visitor = new SelectedIonVisitor(Namespace.FindEntry(DataKind.SelectedIons)?.ColumnMappings);
                visitor.Visit(recs);
                selectedIons.AddRange(visitor.Values);
                break;
            }
        }

        return new SpectrumDescription(rec, scanRecs, precursorInfos, selectedIons);
    }

    public async Task InitializeTables()
    {
        ChunkedArray? spectra = null, scans = null, precursors = null, selectedIons = null;
        var handle = Namespace.OpenMetadata();
        if (handle != null)
            spectra = await ReadAllRowsOf(handle);
        handle = Namespace.OpenScans();
        if (handle != null)
            scans = await ReadAllRowsOf(handle);
        handle = Namespace.OpenPrecursors();
        if (handle != null)
            precursors = await ReadAllRowsOf(handle);
        handle = Namespace.OpenSelectedIons();
        if (handle != null)
            selectedIons = await ReadAllRowsOf(handle);

        if (spectra != null && spectra?.Length > 0)
            SpectrumMetadata = spectra;

        if (scans != null && scans.Length > 0)
            ScanMetadata = scans;
        if (precursors != null && precursors.Length > 0)
            PrecursorMetadata = precursors;
        if (selectedIons != null && selectedIons.Length > 0)
            SelectedIonMetadata = selectedIons;
        // Trigger the population of indices
        if (spectrumMetadata != null)
        {
            NumberOfDataPointsFor(0);
            NumberOfPeaks(0);
        }
    }

    /// <summary>Gets the spectrum description for the specified index.</summary>
    /// <param name="index">The spectrum index.</param>
    public override SpectrumDescription Get(ulong index)
    {
        return GetSpectrum(index);
    }
}

/// <summary>
/// Reader for chromatogram metadata from Parquet files.
/// </summary>
public class ChromatogramMetadataReader : MetadataReaderBase<ChromatogramDescription>
{
    ChunkedArray? chromatogramMetadata = null;
    ChunkedArray? precursorMetadata = null;
    ChunkedArray? selectedIonMetadata = null;

    /// <summary>Gets the number of chromatograms.</summary>
    public override long Length
    {
        get
        {
            if (ChromatogramMetadata == null)
            {
                InitializeTables().Wait();
            }
            return ChromatogramMetadata == null ? 0 : ChromatogramMetadata.Length;
        }
    }

    protected override ChunkedArray? MainTable => ChromatogramMetadata;

    /// <summary>Creates a chromatogram metadata reader.</summary>
    /// <param name="fileReader">The Parquet file reader.</param>
    /// <param name="initializeFacets">Whether to initialize tables immediately.</param>
    public ChromatogramMetadataReader(MzPeakFacetNamespace parquetNamespace, bool initializeFacets = true) : base(MzPeakMetadata.FromFileIndex(parquetNamespace.FileIndex), parquetNamespace)
    {
        if (initializeFacets)
        {
            InitializeTables().Wait();
        }
    }

    /// <summary>Gets or sets the chromatogram metadata table.</summary>
    public ChunkedArray? ChromatogramMetadata
    {
        get
        {
            if (chromatogramMetadata == null)
            {
                InitializeTables().Wait();
            }
            return chromatogramMetadata;
        }
        set => chromatogramMetadata = value;
    }

    /// <summary>Gets or sets the precursor metadata table.</summary>
    public ChunkedArray? PrecursorMetadata
    {
        get
        {
            if (precursorMetadata == null)
            {
                InitializeTables().Wait();
            }
            return precursorMetadata;
        }
        set => precursorMetadata = value;
    }

    /// <summary>Gets or sets the selected ion metadata table.</summary>
    public ChunkedArray? SelectedIonMetadata
    {
        get
        {
            if (selectedIonMetadata == null)
            {
                InitializeTables().Wait();
            }
            return selectedIonMetadata;
        }
        set => selectedIonMetadata = value;
    }

    /// <summary>Loads all chromatogram descriptions.</summary>
    public override List<ChromatogramDescription> BulkLoad()
    {
        if (ChromatogramMetadata == null) return new();
        var recs = new List<ChromatogramInfo>();
        for (var i = 0; i < ChromatogramMetadata.ArrayCount; i++)
        {
            var vis = new ChromatogramVisitor(Namespace.FindEntry(DataKind.Metadata)?.ColumnMappings);
            vis.Visit(ChromatogramMetadata.Array(i));
            recs.AddRange(vis.Values);
        }
        var descrs = recs.Select(s => new ChromatogramDescription(s, new(), new())).ToList();
        if (PrecursorMetadata != null)
        {
            for (var i = 0; i < PrecursorMetadata.ArrayCount; i++)
            {
                var vis = new PrecursorVisitor(Namespace.FindEntry(DataKind.Precursors)?.ColumnMappings);
                vis.Visit(PrecursorMetadata.Array(i));
                foreach (var rec in vis.Values)
                {
                    descrs[(int)rec.SourceIndex].Precursors.Add(rec);
                }
            }
        }
        if (SelectedIonMetadata != null)
        {
            for (var i = 0; i < SelectedIonMetadata.ArrayCount; i++)
            {
                var vis = new SelectedIonVisitor(Namespace.FindEntry(DataKind.SelectedIons)?.ColumnMappings);
                vis.Visit(SelectedIonMetadata.Array(i));
                foreach (var rec in vis.Values)
                {
                    descrs[(int)rec.SourceIndex].SelectedIons.Add(rec);
                }
            }
        }
        return descrs;
    }

    ChromatogramDescription GetChromatogram(ulong index)
    {
        if (ChromatogramMetadata == null) throw new IndexOutOfRangeException($"{index} out of chromatogram index range");
        UInt64Array idxArr;
        ChromatogramInfo? rec = null;
        for (var i = 0; i < ChromatogramMetadata.ArrayCount; i++)
        {
            var chunk = (StructArray)ChromatogramMetadata.Array(i);
            idxArr = (UInt64Array)chunk.Fields[0];
            var first = Compute.Compute.FirstNotNull(idxArr);
            var last = Compute.Compute.LastNotNull(idxArr);
            if (last == null || first == null || first.Value.Item1 > index || last.Value.Item1 < index) continue;
            var mask = Compute.Compute.Equal(idxArr, index);
            var recs = Compute.Compute.Filter(chunk, mask);
            var visitor = new ChromatogramVisitor(Namespace.FindEntry(DataKind.Metadata)?.ColumnMappings);
            visitor.Visit(recs);
            rec = visitor.Values[0];
            break;
        }
        if (rec == null) throw new IndexOutOfRangeException($"{index} out of chromatogram index range");

        List<PrecursorInfo> precursorInfos = new();
        if (PrecursorMetadata != null)
        {
            for (var i = 0; i < PrecursorMetadata.ArrayCount; i++)
            {
                var chunk = (StructArray)PrecursorMetadata.Array(i);
                idxArr = (UInt64Array)chunk.Fields[0];
                var first = Compute.Compute.FirstNotNull(idxArr);
                var last = Compute.Compute.LastNotNull(idxArr);
                if (last == null || first == null || first.Value.Item1 > index || last.Value.Item1 < index) continue;
                var mask = Compute.Compute.Equal(idxArr, index);
                var recs = Compute.Compute.Filter(chunk, mask);
                var visitor = new PrecursorVisitor(Namespace.FindEntry(DataKind.Precursors)?.ColumnMappings);
                visitor.Visit(recs);
                precursorInfos.AddRange(visitor.Values);
                break;
            }
        }
        List<SelectedIonInfo> selectedIons = new();
        if (SelectedIonMetadata != null)
        {
            for (var i = 0; i < SelectedIonMetadata.ArrayCount; i++)
            {
                var chunk = (StructArray)SelectedIonMetadata.Array(i);
                idxArr = (UInt64Array)chunk.Fields[0];
                var first = Compute.Compute.FirstNotNull(idxArr);
                var last = Compute.Compute.LastNotNull(idxArr);
                if (last == null || first == null || first.Value.Item1 > index || last.Value.Item1 < index) continue;
                var mask = Compute.Compute.Equal(idxArr, index);
                var recs = Compute.Compute.Filter(chunk, mask);
                var visitor = new SelectedIonVisitor(Namespace.FindEntry(DataKind.SelectedIons)?.ColumnMappings);
                visitor.Visit(recs);
                selectedIons.AddRange(visitor.Values);
                break;
            }
        }
        return new ChromatogramDescription(rec, precursorInfos, selectedIons);
    }

    /// <summary>Initializes metadata tables by reading from the Parquet file.</summary>
    public async Task InitializeTables()
    {
        ChunkedArray? chromatograms = null, precursors = null, selectedIons = null;
        var fileReader = Namespace.OpenMetadata();
        if (fileReader != null)
            chromatograms = await ReadAllRowsOf(fileReader);

        fileReader = Namespace.OpenPrecursors();
        if (fileReader != null)
            precursors = await ReadAllRowsOf(fileReader);

        fileReader = Namespace.OpenSelectedIons();
        if (fileReader != null)
            selectedIons = await ReadAllRowsOf(fileReader);

        if (chromatograms != null && chromatograms.Length > 0)
        {
            ChromatogramMetadata = chromatograms;
        }
        if (precursors != null && precursors.Length > 0)
        {
            PrecursorMetadata = precursors;
        }
        if (selectedIons != null && selectedIons.Length > 0)
        {
            SelectedIonMetadata = selectedIons;
        }

        // Trigger index building
        if (chromatogramMetadata != null)
        {
            NumberOfDataPointsFor(0);
        }
    }

    /// <summary>Gets the chromatogram description for the specified index.</summary>
    /// <param name="index">The chromatogram index.</param>
    public override ChromatogramDescription Get(ulong index)
    {
        return GetChromatogram(index);
    }
}
