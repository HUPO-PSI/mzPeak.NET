namespace MZPeak.Writer;

using Apache.Arrow;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Storage;


public class MZPeakWriter
{
    MzPeakMetadata mzPeakMetadata;
    IMZPeakArchiveWriter Storage;
    Visitors.SpectrumMetadataBuilder SpectrumMetadata;

    public FileDescription FileDescription => mzPeakMetadata.FileDescription;
    public List<InstrumentConfiguration> InstrumentConfigurations => mzPeakMetadata.InstrumentConfigurations;
    public List<Software> Softwares => mzPeakMetadata.Softwares;
    public List<Sample> Samples => mzPeakMetadata.Samples;
    public List<DataProcessingMethod> DataProcessingMethods => mzPeakMetadata.DataProcessingMethods;


    public MZPeakWriter(IMZPeakArchiveWriter storage)
    {
        Storage = storage;
        mzPeakMetadata = new();
        SpectrumMetadata = new();
    }

    public ulong AddSpectrum(
        string id,
        double time,
        string? dataProcessingRef,
        int numberOfAuxiliaryArrays,
        double[]? mzDeltaModel,
        List<Param> spectrumParams
    )
    {
        return SpectrumMetadata.AppendSpectrum(
            id,
            time,
            dataProcessingRef,
            numberOfAuxiliaryArrays,
            mzDeltaModel,
            spectrumParams
        );
    }

    public void AddScan(
        ulong sourceIndex,
        string? instrumentConfigurationRef,
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
        double? ionMobility,
        string? ionMobilityType,
        List<Param> selectedIonParams)
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
        var entry = FileIndexEntry.FromEntityAndData(EntityType.Spectrum, DataKind.Metadata);
        var stream = Storage.OpenStream(entry);
        var managedStream = new ParquetSharp.IO.ManagedOutputStream(stream);

        var writerProps = new ParquetSharp.WriterPropertiesBuilder().Compression(ParquetSharp.Compression.Zstd).EnableDictionary().EnableWritePageIndex();
        var arrowProps = new ParquetSharp.Arrow.ArrowWriterPropertiesBuilder().StoreSchema();

        var writer = new ParquetSharp.Arrow.FileWriter(managedStream, SpectrumMetadata.ArrowSchema(), writerProps.Build(), arrowProps.Build());
        writer.NewBufferedRowGroup();
        writer.WriteBufferedRecordBatch(SpectrumMetadata.Build());
        writer.Close();
    }
}