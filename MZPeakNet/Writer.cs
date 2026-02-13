namespace MZPeak.Writer;

using System.Text.Json;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Storage;
using MZPeak.Writer.Data;
using ParquetSharp.Arrow;

/// <summary>
/// Represents the current state of the writer during file creation.
/// </summary>
public enum WriterState
{
    /// <summary>Initial state.</summary>
    Start = 0,
    /// <summary>Writing spectrum data arrays.</summary>
    SpectrumData = 1,
    /// <summary>Writing spectrum peak data.</summary>
    SpectrumPeakData = 2,
    /// <summary>Writing spectrum metadata.</summary>
    SpectrumMetadata = 3,
    /// <summary>Writing chromatogram data arrays.</summary>
    ChromatogramData = 4,
    /// <summary>Writing chromatogram metadata.</summary>
    ChromatogramMetadata = 5,
    /// <summary>Writing other data.</summary>
    OtherData = 6,
    /// <summary>Writing other metadata.</summary>
    OtherMetadata = 7,
    /// <summary>Writing complete.</summary>
    Done = 999,
}

/// <summary>
/// Writer for creating mzPeak archive files containing mass spectrometry data.
/// </summary>
public class MZPeakWriter : IDisposable
{
    /// <summary>Optional logger for diagnostic output.</summary>
    public static ILogger? Logger = null;

    /// <summary>The current writer state.</summary>
    public WriterState State = WriterState.Start;
    MzPeakMetadata MzPeakMetadata;
    IMZPeakArchiveWriter Storage;
    Visitors.SpectrumMetadataBuilder SpectrumMetadata;
    Visitors.ChromatogramMetadataBuilder ChromatogramMetadata;

    BaseLayoutBuilder SpectrumData;
    BaseLayoutBuilder ChromatogramData;
    BaseLayoutBuilder? SpectrumPeakData = null;

    public bool SpectrumHasArrayType(ArrayType arrayType) => SpectrumData.HasArrayType(arrayType);
    public bool SpectrumPeaksHasArrayType(ArrayType arrayType) => SpectrumPeakData?.HasArrayType(arrayType) ?? false;
    public bool ChromatogramHasArrayType(ArrayType arrayType) => ChromatogramData.HasArrayType(arrayType);

    FileIndexEntry? CurrentEntry;
    FileWriter? CurrentWriter;

    /// <summary>Gets or sets the file description metadata.</summary>
    public FileDescription FileDescription { get => MzPeakMetadata.FileDescription; set => MzPeakMetadata.FileDescription = value; }
    /// <summary>Gets or sets the list of instrument configurations.</summary>
    public List<InstrumentConfiguration> InstrumentConfigurations { get => MzPeakMetadata.InstrumentConfigurations; set => MzPeakMetadata.InstrumentConfigurations = value; }
    /// <summary>Gets or sets the list of software used.</summary>
    public List<Software> Softwares { get => MzPeakMetadata.Softwares; set => MzPeakMetadata.Softwares = value; }
    /// <summary>Gets or sets the list of samples.</summary>
    public List<Sample> Samples { get => MzPeakMetadata.Samples; set => MzPeakMetadata.Samples = value; }
    /// <summary>Gets or sets the list of data processing methods.</summary>
    public List<DataProcessingMethod> DataProcessingMethods { get => MzPeakMetadata.DataProcessingMethods; set => MzPeakMetadata.DataProcessingMethods = value; }
    /// <summary>Gets or sets the run-level metadata.</summary>
    public MSRun Run { get => MzPeakMetadata.Run; set => MzPeakMetadata.Run = value; }

    protected static ArrayIndex DefaultSpectrumArrayIndex(bool useChunked = false)
    {
        var builder = useChunked ? ArrayIndexBuilder.ChunkBuilder(BufferContext.Spectrum) : ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        return builder.Build();
    }

    protected static ArrayIndex DefaultChromatogramArrayIndex(bool useChunked = false)
    {
        var builder = useChunked ? ArrayIndexBuilder.ChunkBuilder(BufferContext.Chromatogram) : ArrayIndexBuilder.PointBuilder(BufferContext.Chromatogram);
        builder.Add(ArrayType.TimeArray, BinaryDataType.Float64, Unit.Minute, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        return builder.Build();
    }

    protected ParquetSharp.SchemaDescriptor TranslateSchema(Schema schema)
    {
        var stream = new MemoryStream();
        var tmp = new FileWriter(new ParquetSharp.IO.ManagedOutputStream(stream), schema);
        tmp.Close();
        stream.Seek(0, SeekOrigin.Begin);
        var reader = new ParquetSharp.ParquetFileReader(stream);
        return reader.FileMetaData.Schema;
    }


    protected ParquetSharp.WriterPropertiesBuilder ConfigureByteShuffleColumnsFrom(ParquetSharp.WriterPropertiesBuilder writerProps, ArrayIndex arrayIndex, ArrayType targetArrayType, Schema schema)
    {
        /* Three-fold API workaround
            Step 1: Translate Arrow schema to Parquet schema using temporary in-memory Parquet file because
                ParquetSharp doesn't have a one-shot conversion method.
            Step 2: Traverse the the index to find entries with metadata matches and then traverse the ParquetSchema
                    which are exact or prefix matches. This makes matching list element columns easier.
            Step 3: Disable the dictionary encoding, because the underlying C++ library **always** falls back to PLAIN encoding
                    when the dictionary page is too large, and THEN set the desired encoding.
        */
        var parquetSchema = TranslateSchema(schema);
        foreach (var arrayType in arrayIndex.Entries)
        {
            if (arrayType.ArrayTypeCURIE == targetArrayType.CURIE() && arrayType.BufferFormat != BufferFormat.ChunkEncoding)
            {
                for (var i = 0; i < parquetSchema.NumColumns; i++)
                {
                    var descr = parquetSchema.Column(i);
                    var path = descr.Path.ToDotString();
                    if (arrayType.Path == path || path.StartsWith(arrayType.Path + "."))
                    {
                        writerProps = writerProps.DisableDictionary(descr.Path).Encoding(descr.Path, ParquetSharp.Encoding.ByteStreamSplit);
                    }
                }
            }
        }
        return writerProps;
    }

    /// <summary>Starts writing spectrum data arrays.</summary>
    public virtual void StartSpectrumData()
    {
        CloseCurrentWriter();
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableStatistics()
            .EnableWritePageIndex()
            .DisableDictionary($"{SpectrumData.LayoutName()}.{SpectrumData.BufferContext.IndexName()}")
            .Encoding(
                $"{SpectrumData.LayoutName()}.{SpectrumData.BufferContext.IndexName()}",
                ParquetSharp.Encoding.DeltaBinaryPacked
            ).DataPagesize(
                (long)Math.Pow(1024, 2)
            ).MaxRowGroupLength(1024 * 1024 * 4);

        var schema = SpectrumData.ArrowSchema();
        writerProps = ConfigureByteShuffleColumnsFrom(writerProps, SpectrumData.ArrayIndex, ArrayType.MZArray, schema);

        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();

        var writer = new FileWriter(managedStream, schema, writerProps.Build(), arrowProps.Build());

        State = WriterState.SpectrumData;
        CurrentWriter = writer;
        CurrentEntry = entry;
    }

    /// <summary>Starts writing chromatogram data arrays.</summary>
    public virtual void StartChromatogramData()
    {
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Chromatogram, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);
        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableStatistics()
            .DataPagesize(1024 * 1024)
            .DisableDictionary($"{ChromatogramData.LayoutName()}.{ChromatogramData.BufferContext.IndexName()}")
            .Encoding(
                $"{ChromatogramData.LayoutName()}.{ChromatogramData.BufferContext.IndexName()}",
                ParquetSharp.Encoding.DeltaBinaryPacked
            )
            .MaxRowGroupLength(1024 * 1024 * 5)
            .EnableWritePageIndex();

        /* Three-fold API workaround
            Step 1: Translate Arrow schema to Parquet schema using temporary in-memory Parquet file because
                   ParquetSharp doesn't have a one-shot conversion method.
            Step 2: Traverse the the index to find entries with metadata matches and then traverse the ParquetSchema
                    which are exact or prefix matches. This makes matching list element columns easier.
            Step 3: Disable the dictionary encoding, because the underlying C++ library **always** falls back to PLAIN encoding
                    when the dictionary page is too large, and THEN set the desired encoding.
        */
        var schema = ChromatogramData.ArrowSchema();
        writerProps = ConfigureByteShuffleColumnsFrom(writerProps, ChromatogramData.ArrayIndex, ArrayType.TimeArray, schema);

        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        CurrentEntry = entry;
        CurrentWriter = new FileWriter(managedStream, schema, writerProps.Build(), arrowProps.Build());
    }

    /// <summary>Closes the current file writer.</summary>
    public virtual void CloseCurrentWriter()
    {
        if (CurrentEntry != null)
        {
            CurrentWriter?.Close();
            CurrentEntry = null;
            CurrentWriter = null;
        }
    }

    public ArrayIndex SpectrumArrayIndex => SpectrumData.ArrayIndex;
    public ArrayIndex ChromatogramArrayIndex => ChromatogramData.ArrayIndex;
    public ArrayIndex? SpectrumPeakArrayIndex => SpectrumPeakData?.ArrayIndex;

    /// <summary>Starts writing spectrum peak data.</summary>
    public virtual void StartSpectrumPeakData()
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException();
        CloseCurrentWriter();
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.Peaks);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableStatistics()
            .EnableWritePageIndex()
            .DisableDictionary($"{SpectrumPeakData.LayoutName()}.{SpectrumPeakData.BufferContext.IndexName()}")
            .Encoding(
                $"{SpectrumPeakData.LayoutName()}.{SpectrumPeakData.BufferContext.IndexName()}",
                ParquetSharp.Encoding.DeltaBinaryPacked
            ).DataPagesize(
                (long)Math.Pow(1024, 2)
            ).MaxRowGroupLength(1024 * 1024 * 4);

        var schema = SpectrumPeakData.ArrowSchema();
        writerProps = ConfigureByteShuffleColumnsFrom(writerProps, SpectrumPeakData.ArrayIndex, ArrayType.MZArray, schema);

        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();

        var writer = new FileWriter(managedStream, schema, writerProps.Build(), arrowProps.Build());

        State = WriterState.SpectrumPeakData;
        CurrentWriter = writer;
        CurrentEntry = entry;
    }

    /// <summary>Creates an mzPeak writer.</summary>
    /// <param name="storage">The archive storage backend.</param>
    /// <param name="spectrumArrayIndex">Optional custom spectrum array index.</param>
    /// <param name="chromatogramArrayIndex">Optional custom chromatogram array index.</param>
    /// <param name="includeSpectrumPeakData">Whether to include spectrum peak data.</param>
    /// <param name="spectrumPeakArrayIndex">Optional custom spectrum peak array index.</param>
    /// <param name="useChunked">Optionally set any default array indices to use chunked encoding. Has no effect on explicitly provided array indices.</param>
    public MZPeakWriter(IMZPeakArchiveWriter storage,
                        ArrayIndex? spectrumArrayIndex = null,
                        ArrayIndex? chromatogramArrayIndex = null,
                        bool includeSpectrumPeakData = false,
                        ArrayIndex? spectrumPeakArrayIndex = null,
                        bool useChunked = false)
    {
        if (spectrumArrayIndex == null)
            spectrumArrayIndex = DefaultSpectrumArrayIndex(useChunked);
        if (chromatogramArrayIndex == null)
            chromatogramArrayIndex = DefaultChromatogramArrayIndex(useChunked);
        Storage = storage;
        MzPeakMetadata = new();
        SpectrumMetadata = new();
        SpectrumData = spectrumArrayIndex.InferBufferFormat() switch {
            BufferFormat.Point => new PointLayoutBuilder(spectrumArrayIndex),
            BufferFormat.ChunkValues => new ChunkLayoutBuilder(spectrumArrayIndex),
            _ => throw new NotImplementedException($"Buffer format {spectrumArrayIndex.InferBufferFormat()} not recognized")
        };
        ChromatogramMetadata = new();
        ChromatogramData = chromatogramArrayIndex.InferBufferFormat() switch
        {
            BufferFormat.Point => new PointLayoutBuilder(chromatogramArrayIndex),
            BufferFormat.ChunkValues => new ChunkLayoutBuilder(chromatogramArrayIndex),
            _ => throw new NotImplementedException($"Buffer format {chromatogramArrayIndex.InferBufferFormat()} not recognized")
        };
        ChromatogramData.ShouldRemoveZeroRuns = false;
        if (includeSpectrumPeakData)
            SpectrumPeakData = new PointLayoutBuilder(spectrumPeakArrayIndex ?? DefaultSpectrumArrayIndex());
        StartSpectrumData();
    }

    /// <summary>Gets the current spectrum index.</summary>
    public ulong CurrentSpectrum => SpectrumMetadata.SpectrumCounter;
    /// <summary>Gets the current chromatogram index.</summary>
    public ulong CurrentChromatogram => ChromatogramMetadata.ChromatogramCounter;

    /// <summary>Adds spectrum data arrays from a dictionary.</summary>
    /// <param name="entryIndex">The spectrum index.</param>
    /// <param name="arrays">Dictionary mapping array index entries to arrays.</param>
    /// <param name="isProfile">Whether the spectrum is profile mode.</param>
    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile = null)
    {
        return SpectrumData.Add(entryIndex, arrays, isProfile);
    }

    /// <summary>Adds spectrum data arrays.</summary>
    /// <param name="entryIndex">The spectrum index.</param>
    /// <param name="arrays">The data arrays to add.</param>
    /// <param name="isProfile">Whether the spectrum is profile mode.</param>
    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, IEnumerable<Array> arrays, bool? isProfile = null)
    {
        return SpectrumData.Add(entryIndex, arrays, isProfile);
    }

    /// <summary>Adds spectrum data from Arrow arrays.</summary>
    /// <param name="entryIndex">The spectrum index.</param>
    /// <param name="arrays">The Arrow arrays to add.</param>
    /// <param name="isProfile">Whether the spectrum is profile mode.</param>
    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, IEnumerable<IArrowArray> arrays, bool? isProfile = null)
    {
        return SpectrumData.Add(entryIndex, arrays, isProfile);
    }

    /// <summary>Adds spectrum peak data from a dictionary.</summary>
    /// <param name="entryIndex">The spectrum index.</param>
    /// <param name="arrays">Dictionary mapping array index entries to arrays.</param>
    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumPeakData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays)
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException("Spectrum peak writing is not enabled");
        return SpectrumPeakData.Add(entryIndex, arrays, false);
    }

    /// <summary>Adds spectrum peak data arrays.</summary>
    /// <param name="entryIndex">The spectrum index.</param>
    /// <param name="arrays">The data arrays to add.</param>
    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumPeakData(ulong entryIndex, IEnumerable<Array> arrays)
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException("Spectrum peak writing is not enabled");
        return SpectrumPeakData.Add(entryIndex, arrays, false);
    }

    /// <summary>Adds spectrum peak data from Arrow arrays.</summary>
    /// <param name="entryIndex">The spectrum index.</param>
    /// <param name="arrays">The Arrow arrays to add.</param>
    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumPeakData(ulong entryIndex, IEnumerable<IArrowArray> arrays)
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException("Spectrum peak writing is not enabled");
        return SpectrumPeakData.Add(entryIndex, arrays, false);
    }

    /// <summary>Adds chromatogram data from a dictionary.</summary>
    /// <param name="entryIndex">The chromatogram index.</param>
    /// <param name="arrays">Dictionary mapping array index entries to arrays.</param>
    public List<AuxiliaryArray> AddChromatogramData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays)
    {
        return ChromatogramData.Add(entryIndex, arrays, isProfile: true).Item2;
    }

    /// <summary>Adds chromatogram data from Arrow arrays.</summary>
    /// <param name="entryIndex">The chromatogram index.</param>
    /// <param name="arrays">The Arrow arrays to add.</param>
    public List<AuxiliaryArray> AddChromatogramData(ulong entryIndex, IEnumerable<IArrowArray> arrays)
    {
        return ChromatogramData.Add(entryIndex, arrays, isProfile: true).Item2;
    }

    /// <summary>Adds chromatogram data arrays.</summary>
    /// <param name="entryIndex">The chromatogram index.</param>
    /// <param name="arrays">The data arrays to add.</param>
    public List<AuxiliaryArray> AddChromatogramData(ulong entryIndex, IEnumerable<Array> arrays)
    {
        return ChromatogramData.Add(entryIndex, arrays, isProfile: true).Item2;
    }

    /// <summary>Flushes buffered spectrum data to the output.</summary>
    public virtual void FlushSpectrumData()
    {
        if (State == WriterState.SpectrumData && CurrentWriter != null)
        {
            var batch = SpectrumData.GetRecordBatch();
            CurrentWriter?.WriteBufferedRecordBatch(batch);
        }
        else if (SpectrumData.BufferedRows > 0)
        {
            throw new InvalidOperationException($"Attempting to flush the spectrum data buffer while the current entry is {CurrentEntry}");
        }
    }

    /// <summary>Flushes buffered spectrum peak data to the output.</summary>
    public virtual void FlushSpectrumPeakData()
    {
        if (State == WriterState.SpectrumPeakData && SpectrumPeakData != null && CurrentWriter != null)
        {
            var batch = SpectrumPeakData.GetRecordBatch();
            CurrentWriter?.WriteBufferedRecordBatch(batch);
        }
        else if (SpectrumData.BufferedRows > 0)
        {
            throw new InvalidOperationException($"Attempting to flush the spectrum peak data buffer while the current entry is {CurrentEntry}");
        }
    }

    /// <summary>Adds a spectrum entry with metadata.</summary>
    /// <param name="id">The spectrum native ID.</param>
    /// <param name="time">The retention time.</param>
    /// <param name="dataProcessingRef">Optional data processing reference.</param>
    /// <param name="mzDeltaModel">Optional m/z delta interpolation model coefficients.</param>
    /// <param name="spectrumParams">Optional spectrum parameters.</param>
    /// <param name="auxiliaryArrays">Optional auxiliary arrays.</param>
    public ulong AddSpectrum(
        string id,
        double time,
        string? dataProcessingRef,
        List<double>? mzDeltaModel = null,
        List<Param>? spectrumParams = null,
        List<AuxiliaryArray>? auxiliaryArrays = null
    )
    {
        return SpectrumMetadata.AppendSpectrum(
            id,
            time,
            dataProcessingRef,
            mzDeltaModel,
            spectrumParams ?? new(),
            auxiliaryArrays
        );
    }

    /// <summary>Adds a scan entry to a spectrum.</summary>
    /// <param name="sourceIndex">The parent spectrum index.</param>
    /// <param name="instrumentConfigurationRef">Optional instrument configuration reference.</param>
    /// <param name="scanParams">Scan parameters.</param>
    /// <param name="ionMobility">Optional ion mobility value.</param>
    /// <param name="ionMobilityType">Optional ion mobility type CURIE.</param>
    /// <param name="scanWindows">Optional scan windows parameters.</param>
    public void AddScan(
        ulong sourceIndex,
        uint? instrumentConfigurationRef,
        List<Param> scanParams,
        double? ionMobility = null,
        string? ionMobilityType = null,
        List<List<Param>>? scanWindows = null
    )
    {
        SpectrumMetadata.AppendScan(
            sourceIndex,
            instrumentConfigurationRef,
            ionMobility,
            ionMobilityType,
            scanParams,
            scanWindows
        );
    }

    /// <summary>Adds a precursor entry to a spectrum.</summary>
    /// <param name="sourceIndex">The parent spectrum index.</param>
    /// <param name="precursorIndex">The precursor spectrum index.</param>
    /// <param name="precursorId">Optional precursor spectrum ID.</param>
    /// <param name="isolationWindowParams">Isolation window parameters.</param>
    /// <param name="activationParams">Activation parameters.</param>
    public void AddPrecursor(
        ulong sourceIndex,
        ulong precursorIndex,
        string? precursorId,
        List<Param> isolationWindowParams,
        List<Param> activationParams
    )
    {
        SpectrumMetadata.AppendPrecursor(
            sourceIndex,
            precursorIndex,
            precursorId,
            isolationWindowParams,
            activationParams
        );
    }

    /// <summary>Adds a selected ion entry to a precursor.</summary>
    /// <param name="sourceIndex">The parent spectrum index.</param>
    /// <param name="precursorIndex">The precursor index.</param>
    /// <param name="selectedIonParams">Selected ion parameters.</param>
    /// <param name="ionMobility">Optional ion mobility value.</param>
    /// <param name="ionMobilityType">Optional ion mobility type CURIE.</param>
    public void AddSelectedIon(
        ulong sourceIndex,
        ulong precursorIndex,
        List<Param> selectedIonParams,
        double? ionMobility = null,
        string? ionMobilityType = null
    )
    {
        SpectrumMetadata.AppendSelectedIon(
            sourceIndex,
            precursorIndex,
            ionMobility,
            ionMobilityType,
            selectedIonParams
        );
    }

    /// <summary>Adds a chromatogram entry with metadata.</summary>
    /// <param name="id">The chromatogram native ID.</param>
    /// <param name="dataProcessingRef">Optional data processing reference.</param>
    /// <param name="chromatogramParams">Optional chromatogram parameters.</param>
    /// <param name="auxiliaryArrays">Optional auxiliary arrays.</param>
    public ulong AddChromatogram(
        string id,
        string? dataProcessingRef,
        List<Param>? chromatogramParams = null,
        List<AuxiliaryArray>? auxiliaryArrays = null
    )
    {
        return ChromatogramMetadata.AppendChromatogram(id, dataProcessingRef, chromatogramParams ?? new(), auxiliaryArrays);
    }

    /// <summary>Adds a precursor entry to a chromatogram.</summary>
    /// <param name="sourceIndex">The parent chromatogram index.</param>
    /// <param name="precursorIndex">The precursor chromatogram index.</param>
    /// <param name="precursorId">Optional precursor chromatogram ID.</param>
    /// <param name="isolationWindowParams">Isolation window parameters.</param>
    /// <param name="activationParams">Activation parameters.</param>
    public void AddChromatogramPrecursor(
        ulong sourceIndex,
        ulong precursorIndex,
        string? precursorId,
        List<Param> isolationWindowParams,
        List<Param> activationParams
    )
    {
        ChromatogramMetadata.AppendPrecursor(
            sourceIndex,
            precursorIndex,
            precursorId,
            isolationWindowParams,
            activationParams
        );
    }

    /// <summary>Adds a selected ion entry to a precursor.</summary>
    /// <param name="sourceIndex">The parent chromatogram index.</param>
    /// <param name="precursorIndex">The precursor index.</param>
    /// <param name="selectedIonParams">Selected ion parameters.</param>
    /// <param name="ionMobility">Optional ion mobility value.</param>
    /// <param name="ionMobilityType">Optional ion mobility type CURIE.</param>
    public void AddChromatogramSelectedIon(
        ulong sourceIndex,
        ulong precursorIndex,
        List<Param> selectedIonParams,
        double? ionMobility = null,
        string? ionMobilityType = null
    )
    {
        ChromatogramMetadata.AppendSelectedIon(
            sourceIndex,
            precursorIndex,
            ionMobility,
            ionMobilityType,
            selectedIonParams
        );
    }


    /// <summary>Writes spectrum metadata to the archive.</summary>
    public void WriteSpectrumMetadata()
    {
        if (State > WriterState.SpectrumMetadata)
            return;
        CloseCurrentWriter();
        State = WriterState.SpectrumMetadata;
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.Metadata);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);
        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .DisableDictionary("spectrum.index")
            .DisableDictionary("scan.source_index")
            .DisableDictionary("precursor.source_index")
            .DisableDictionary("precursor.precursor_index")
            .DisableDictionary("selected_ion.source_index")
            .DisableDictionary("selected_ion.precursor_index")
            .Encoding("spectrum.index", ParquetSharp.Encoding.DeltaBinaryPacked)
            .Encoding("scan.source_index", ParquetSharp.Encoding.DeltaBinaryPacked)
            .Encoding("precursor.source_index", ParquetSharp.Encoding.DeltaBinaryPacked)
            .Encoding("precursor.precursor_index", ParquetSharp.Encoding.DeltaBinaryPacked)
            .Encoding("selected_ion.source_index", ParquetSharp.Encoding.DeltaBinaryPacked)
            .Encoding("selected_ion.precursor_index", ParquetSharp.Encoding.DeltaBinaryPacked)
            .EnableStatistics()
            .EnableWritePageIndex();
        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        CurrentEntry = entry;

        var meta = new Dictionary<string, string>();
        meta["file_description"] = JsonSerializer.Serialize(FileDescription);
        meta["instrument_configuration_list"] = JsonSerializer.Serialize(InstrumentConfigurations);
        meta["data_processing_method_list"] = JsonSerializer.Serialize(DataProcessingMethods);
        meta["software_list"] = JsonSerializer.Serialize(Softwares);
        meta["sample_list"] = JsonSerializer.Serialize(Samples);
        meta["run"] = JsonSerializer.Serialize(Run);
        meta["spectrum_count"] = SpectrumMetadata.Length.ToString();
        meta["spectrum_data_point_count"] = SpectrumData.NumberOfPoints.ToString();

        CurrentWriter = new FileWriter(
            managedStream,
            SpectrumMetadata.ArrowSchema(meta),
            writerProps.Build(),
            arrowProps.Build()
        );
        CurrentWriter.NewBufferedRowGroup();
        var batch = SpectrumMetadata.Build();
        CurrentWriter.WriteBufferedRecordBatch(batch);
        CloseCurrentWriter();
    }


    /// <summary>Writes chromatogram data to the archive.</summary>
    public void WriteChromatogramData()
    {
        if (State > WriterState.ChromatogramData)
            return;
        State = WriterState.ChromatogramData;
        if (ChromatogramData.BufferedRows == 0) return;
        StartChromatogramData();
        var batch = ChromatogramData.GetRecordBatch();
        CurrentWriter?.WriteBufferedRecordBatch(batch);
        CloseCurrentWriter();
    }

    /// <summary>Writes chromatogram metadata to the archive.</summary>
    public void WriteChromatogramMetadata()
    {
        if (State > WriterState.ChromatogramMetadata)
            return;
        State = WriterState.ChromatogramMetadata;
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Chromatogram, DataKind.Metadata);
        var stream = Storage.OpenStream(entry);

        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableStatistics()
            .EnableWritePageIndex();
        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();

        var meta = new Dictionary<string, string>();
        meta["file_description"] = JsonSerializer.Serialize(FileDescription);
        meta["instrument_configuration_list"] = JsonSerializer.Serialize(InstrumentConfigurations);
        meta["data_processing_method_list"] = JsonSerializer.Serialize(DataProcessingMethods);
        meta["software_list"] = JsonSerializer.Serialize(Softwares);
        meta["sample_list"] = JsonSerializer.Serialize(Samples);
        meta["run"] = JsonSerializer.Serialize(Run);
        meta["chromatogram_count"] = ChromatogramMetadata.Length.ToString();
        meta["chromatogram_data_point_count"] = ChromatogramData.NumberOfPoints.ToString();

        CurrentEntry = entry;
        CurrentWriter = new FileWriter(managedStream, ChromatogramMetadata.ArrowSchema(meta), writerProps.Build(), arrowProps.Build());
        CurrentWriter.NewBufferedRowGroup();
        var batch = ChromatogramMetadata.Build();
        CurrentWriter.WriteBufferedRecordBatch(batch);
        CloseCurrentWriter();
    }

    /// <summary>Closes the writer and finalizes the archive.</summary>
    public void Close()
    {
        FlushSpectrumData();
        if (State == WriterState.SpectrumData)
            CloseCurrentWriter();
        if (SpectrumPeakData != null)
        {
            StartSpectrumPeakData();
            FlushSpectrumPeakData();
            CloseCurrentWriter();
        }
        WriteSpectrumMetadata();
        WriteChromatogramData();
        WriteChromatogramMetadata();
        Storage.Dispose();
    }

    /// <summary>Disposes resources and closes the writer.</summary>
    public void Dispose()
    {
        Close();
    }
}