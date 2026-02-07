namespace MZPeak.Writer;

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
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

    protected static ArrayIndex DefaultSpectrumArrayIndex()
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        return builder.Build();
    }

    protected static ArrayIndex DefaultChromatogramArrayIndex()
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Chromatogram);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.Minute, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        return builder.Build();
    }

    /// <summary>Starts writing spectrum data arrays.</summary>
    public void StartSpectrumData()
    {
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableStatistics()
            .EnableWritePageIndex()
            .Encoding(
                $"{SpectrumData.LayoutName}.{SpectrumData.BufferContext.IndexName()}",
                ParquetSharp.Encoding.DeltaBinaryPacked
            );

        foreach (var arrayType in SpectrumData.ArrayIndex.Entries)
        {
            if (arrayType.GetArrayType() == ArrayType.MZArray)
            {
                writerProps = writerProps.Encoding(arrayType.Path, ParquetSharp.Encoding.ByteStreamSplit);
            }
        }

        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        var schema = SpectrumData.ArrowSchema();
        var writer = new FileWriter(managedStream, schema, writerProps.Build(), arrowProps.Build());

        State = WriterState.SpectrumData;
        CurrentWriter = writer;
        CurrentEntry = entry;
    }

    /// <summary>Closes the current file writer.</summary>
    public void CloseCurrentWriter()
    {
        if (CurrentEntry != null)
        {
            CurrentWriter?.Close();
            CurrentEntry = null;
            CurrentWriter = null;
        }
    }

    /// <summary>Starts writing spectrum peak data.</summary>
    public void StartSpectrumPeakData()
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException();
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.Peaks);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableStatistics()
            .EnableWritePageIndex()
            .Encoding(
                $"{SpectrumPeakData.LayoutName}.{SpectrumPeakData.BufferContext.IndexName()}",
                ParquetSharp.Encoding.DeltaBinaryPacked
            );

        foreach (var arrayType in SpectrumPeakData.ArrayIndex.Entries)
        {
            if (arrayType.GetArrayType() == ArrayType.MZArray)
                writerProps = writerProps.Encoding(arrayType.Path, ParquetSharp.Encoding.ByteStreamSplit);
        }

        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        var schema = SpectrumPeakData.ArrowSchema();
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
    public MZPeakWriter(IMZPeakArchiveWriter storage, ArrayIndex? spectrumArrayIndex = null, ArrayIndex? chromatogramArrayIndex = null, bool includeSpectrumPeakData = false, ArrayIndex? spectrumPeakArrayIndex = null)
    {
        Storage = storage;
        MzPeakMetadata = new();
        SpectrumMetadata = new();
        SpectrumData = new PointLayoutBuilder(spectrumArrayIndex ?? DefaultSpectrumArrayIndex());
        ChromatogramMetadata = new();
        ChromatogramData = new PointLayoutBuilder(chromatogramArrayIndex ?? DefaultChromatogramArrayIndex());
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
    public void FlushSpectrumData()
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
    public void FlushSpectrumPeakData()
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
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Chromatogram, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);
        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableStatistics()
            .EnableWritePageIndex();
        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        CurrentEntry = entry;
        CurrentWriter = new FileWriter(managedStream, ChromatogramData.ArrowSchema(), writerProps.Build(), arrowProps.Build());
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