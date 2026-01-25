namespace MZPeak.Writer;

using Apache.Arrow;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Storage;
using MZPeak.Writer.Data;
using ParquetSharp.Arrow;

public class MZPeakWriter : IDisposable
{
    MzPeakMetadata mzPeakMetadata;
    IMZPeakArchiveWriter Storage;
    Visitors.SpectrumMetadataBuilder SpectrumMetadata;
    BaseLayoutBuilder SpectrumData;

    FileIndexEntry? CurrentEntry;
    FileWriter CurrentWriter;

    public FileDescription FileDescription => mzPeakMetadata.FileDescription;
    public List<InstrumentConfiguration> InstrumentConfigurations => mzPeakMetadata.InstrumentConfigurations;
    public List<Software> Softwares => mzPeakMetadata.Softwares;
    public List<Sample> Samples => mzPeakMetadata.Samples;
    public List<DataProcessingMethod> DataProcessingMethods => mzPeakMetadata.DataProcessingMethods;


    protected static ArrayIndex DefaultSpectrumArrayIndex()
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        return builder.Build();
    }

    public (FileWriter, FileIndexEntry) StartSpectrumData()
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
        return (writer, entry);
    }

    public void CloseCurrentWriter()
    {
        if (CurrentEntry != null)
        {
            CurrentWriter.Close();
            CurrentEntry = null;
        }
    }

    public MZPeakWriter(IMZPeakArchiveWriter storage, ArrayIndex spectrumArrayIndex)
    {
        Storage = storage;
        mzPeakMetadata = new();
        SpectrumMetadata = new();
        SpectrumData = new PointLayoutBuilder(spectrumArrayIndex);
        (CurrentWriter, CurrentEntry) = StartSpectrumData();
    }

    public MZPeakWriter(IMZPeakArchiveWriter storage) : this(storage, DefaultSpectrumArrayIndex())
    {}

    public ulong CurrentSpectrum => SpectrumMetadata.SpectrumCounter;

    public void AddSpectrumData(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays)
    {
        SpectrumData.Add(entryIndex, arrays);
    }

    public void AddSpectrumData(ulong entryIndex, IEnumerable<Array> arrays)
    {
        SpectrumData.Add(entryIndex, arrays);
    }

    public void FlushSpectrumData()
    {
        if (CurrentEntry?.DataKind == DataKind.DataArrays && CurrentEntry?.EntityType == EntityType.Spectrum)
        {
            var batch = SpectrumData.GetRecordBatch();
            CurrentWriter.WriteBufferedRecordBatch(batch);
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
        List<double>? mzDeltaModel,
        List<Param> spectrumParams,
        List<AuxiliaryArray>? auxiliaryArrays=null
    )
    {
        return SpectrumMetadata.AppendSpectrum(
            id,
            time,
            dataProcessingRef,
            mzDeltaModel,
            spectrumParams,
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
        double? ionMobility=null,
        string? ionMobilityType=null
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
        CloseCurrentWriter();
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.Metadata);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder().Compression(ParquetSharp.Compression.Zstd).EnableDictionary().EnableWritePageIndex();
        var arrowProps = new ArrowWriterPropertiesBuilder().StoreSchema();

        var writer = new FileWriter(managedStream, SpectrumMetadata.ArrowSchema(), writerProps.Build(), arrowProps.Build());
        writer.NewBufferedRowGroup();
        writer.WriteBufferedRecordBatch(SpectrumMetadata.Build());
        writer.Close();
    }

    public void Dispose()
    {
        Storage.Dispose();
    }
}