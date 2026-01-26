namespace MZPeak.Writer;

using System.Text.Json;
using Apache.Arrow;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Storage;
using MZPeak.Writer.Data;
using ParquetSharp.Arrow;

public enum WriterState
{
    Start = 0,
    SpectrumData,
    SpectrumMetadata,
    ChromatogramData,
    ChromatogramMetadata,
    Done = 999,
}

public class MZPeakWriter : IDisposable
{
    public WriterState State = WriterState.Start;
    MzPeakMetadata MzPeakMetadata;
    IMZPeakArchiveWriter Storage;
    Visitors.SpectrumMetadataBuilder SpectrumMetadata;
    BaseLayoutBuilder SpectrumData;

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

    public void StartSpectrumData()
    {
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder()
            .Compression(ParquetSharp.Compression.Zstd)
            .EnableDictionary()
            .EnableWritePageIndex();

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

    public MZPeakWriter(IMZPeakArchiveWriter storage, ArrayIndex spectrumArrayIndex)
    {
        Storage = storage;
        MzPeakMetadata = new();
        SpectrumMetadata = new();
        SpectrumData = new PointLayoutBuilder(spectrumArrayIndex);
        StartSpectrumData();
    }

    public MZPeakWriter(IMZPeakArchiveWriter storage) : this(storage, DefaultSpectrumArrayIndex())
    { }

    public ulong CurrentSpectrum => SpectrumMetadata.SpectrumCounter;

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays)
    {
        return SpectrumData.Add(entryIndex, arrays);
    }

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, IEnumerable<Array> arrays)
    {
        return SpectrumData.Add(entryIndex, arrays);
    }

    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) AddSpectrumData(ulong entryIndex, IEnumerable<IArrowArray> arrays)
    {
        return SpectrumData.Add(entryIndex, arrays);
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
        double? ionMobility,
        string? ionMobilityType,
        List<Param> scanParams
    )
    {
        SpectrumMetadata.AppendScan(
            sourceIndex,
            instrumentConfigurationRef,
            ionMobility,
            ionMobilityType,
            scanParams
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
        // meta["run"] = JsonSerializer.Serialize()

        CurrentWriter = new FileWriter(
            managedStream,
            SpectrumMetadata.ArrowSchema(meta),
            writerProps.Build(),
            arrowProps.Build()
        );
        CurrentWriter.NewBufferedRowGroup();
        CurrentWriter.WriteBufferedRecordBatch(SpectrumMetadata.Build());
        CloseCurrentWriter();
    }

    public void WriteChromatogramData()
    {
        if (State > WriterState.ChromatogramData)
            return;
        State = WriterState.ChromatogramData;
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Chromatogram, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);
        var writerProps = new ParquetSharp.WriterPropertiesBuilder().Compression(ParquetSharp.Compression.Zstd).EnableDictionary().EnableWritePageIndex();
        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        CurrentEntry = entry;
        // CurrentWriter = new FileWriter(managedStream, SpectrumMetadata.ArrowSchema(), writerProps.Build(), arrowProps.Build());
        CloseCurrentWriter();
    }

    public void WriteChromatogramMetadata()
    {
        if (State > WriterState.ChromatogramMetadata)
            return;
        State = WriterState.ChromatogramMetadata;
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Chromatogram, DataKind.DataArrays);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);
        var writerProps = new ParquetSharp.WriterPropertiesBuilder().Compression(ParquetSharp.Compression.Zstd).EnableDictionary().EnableWritePageIndex();
        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();
        CurrentEntry = entry;
        CloseCurrentWriter();
    }

    public void Close()
    {
        FlushSpectrumData();
        if (State == WriterState.SpectrumData)
            CloseCurrentWriter();
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