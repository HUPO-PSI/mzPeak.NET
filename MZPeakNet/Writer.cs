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

public enum WriterState
{
    Start = 0,
    SpectrumData = 1,
    SpectrumPeakData = 2,
    SpectrumMetadata = 3,
    ChromatogramData = 4,
    ChromatogramMetadata = 5,
    OtherData = 6,
    OtherMetadata = 7,
    Done = 999,
}

public class MZPeakWriter : IDisposable
{
    public static ILogger? Logger = null;

    public WriterState State = WriterState.Start;
    MzPeakMetadata MzPeakMetadata;
    IMZPeakArchiveWriter Storage;
    Visitors.SpectrumMetadataBuilder SpectrumMetadata;
    Visitors.ChromatogramMetadataBuilder ChromatogramMetadata;

    BaseLayoutBuilder SpectrumData;
    BaseLayoutBuilder ChromatogramData;
    BaseLayoutBuilder? SpectrumPeakData = null;

    FileIndexEntry? CurrentEntry;
    FileWriter? CurrentWriter;

    public FileDescription FileDescription { get => MzPeakMetadata.FileDescription; set => MzPeakMetadata.FileDescription = value; }
    public List<InstrumentConfiguration> InstrumentConfigurations { get => MzPeakMetadata.InstrumentConfigurations; set => MzPeakMetadata.InstrumentConfigurations = value; }
    public List<Software> Softwares { get => MzPeakMetadata.Softwares; set => MzPeakMetadata.Softwares = value; }
    public List<Sample> Samples { get => MzPeakMetadata.Samples; set => MzPeakMetadata.Samples = value; }
    public List<DataProcessingMethod> DataProcessingMethods { get => MzPeakMetadata.DataProcessingMethods; set => MzPeakMetadata.DataProcessingMethods = value; }
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

    public void StartSpectrumData()
    {
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableWritePageIndex()
            .Encoding(
                $"{SpectrumData.LayoutName}.{SpectrumData.BufferContext.IndexName()}",
                ParquetSharp.Encoding.DeltaBinaryPacked
            );

        foreach(var arrayType in SpectrumData.ArrayIndex.Entries)
        {
            if(arrayType.GetArrayType() == ArrayType.MZArray)
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

    public void CloseCurrentWriter()
    {
        if (CurrentEntry != null)
        {
            CurrentWriter?.Close();
            CurrentEntry = null;
            CurrentWriter = null;
        }
    }

    public void StartSpectrumPeakData()
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException();
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.Peaks);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
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

    public MZPeakWriter(IMZPeakArchiveWriter storage, ArrayIndex? spectrumArrayIndex=null, ArrayIndex? chromatogramArrayIndex=null, bool includeSpectrumPeakData=false, ArrayIndex? spectrumPeakArrayIndex=null)
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

    public ulong CurrentSpectrum => SpectrumMetadata.SpectrumCounter;
    public ulong CurrentChromatogram => ChromatogramMetadata.ChromatogramCounter;

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile = null)
    {
        return SpectrumData.Add(entryIndex, arrays, isProfile);
    }

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, IEnumerable<Array> arrays, bool? isProfile = null)
    {
        return SpectrumData.Add(entryIndex, arrays, isProfile);
    }

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, IEnumerable<IArrowArray> arrays, bool? isProfile = null)
    {
        return SpectrumData.Add(entryIndex, arrays, isProfile);
    }

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumPeakData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays)
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException("Spectrum peak writing is not enabled");
        return SpectrumPeakData.Add(entryIndex, arrays, false);
    }

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumPeakData(ulong entryIndex, IEnumerable<Array> arrays)
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException("Spectrum peak writing is not enabled");
        return SpectrumPeakData.Add(entryIndex, arrays, false);
    }

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumPeakData(ulong entryIndex, IEnumerable<IArrowArray> arrays)
    {
        if (SpectrumPeakData == null) throw new InvalidOperationException("Spectrum peak writing is not enabled");
        return SpectrumPeakData.Add(entryIndex, arrays, false);
    }

    public List<AuxiliaryArray> AddChromatogramData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays)
    {
        return ChromatogramData.Add(entryIndex, arrays, isProfile: true).Item2;
    }

    public List<AuxiliaryArray> AddChromatogramData(ulong entryIndex, IEnumerable<IArrowArray> arrays)
    {
        return ChromatogramData.Add(entryIndex, arrays, isProfile: true).Item2;
    }

    public List<AuxiliaryArray> AddChromatogramData(ulong entryIndex, IEnumerable<Array> arrays)
    {
        return ChromatogramData.Add(entryIndex, arrays, isProfile: true).Item2;
    }

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

    public ulong AddSpectrum(
        string id,
        double time,
        string? dataProcessingRef,
        List<double>? mzDeltaModel=null,
        List<Param>? spectrumParams=null,
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

    public void AddScan(
        ulong sourceIndex,
        uint? instrumentConfigurationRef,
        List<Param> scanParams,
        double? ionMobility=null,
        string? ionMobilityType=null,
        List<List<Param>>? scanWindows=null
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

    public ulong AddChromatogram(
        string id,
        string? dataProcessingRef,
        List<Param>? chromatogramParams = null,
        List<AuxiliaryArray>? auxiliaryArrays = null
    )
    {
        return ChromatogramMetadata.AppendChromatogram(id, dataProcessingRef, chromatogramParams ?? new(), auxiliaryArrays);
    }

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
            .EnableWritePageIndex();
        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        CurrentEntry = entry;
        CurrentWriter = new FileWriter(managedStream, ChromatogramData.ArrowSchema(), writerProps.Build(), arrowProps.Build());
        CloseCurrentWriter();
    }

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

    public void Close()
    {
        FlushSpectrumData();
        if (State == WriterState.SpectrumData)
            CloseCurrentWriter();
        if (SpectrumPeakData != null)
        {
            StartSpectrumPeakData();
        }
        WriteSpectrumMetadata();
        WriteChromatogramData();
        WriteChromatogramMetadata();
        Storage.Dispose();
    }

    public void Dispose()
    {
        Close();
    }
}