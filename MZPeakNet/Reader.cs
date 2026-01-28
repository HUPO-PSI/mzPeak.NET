
using Apache.Arrow;
using MZPeak.Metadata;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;

namespace MZPeak.Reader;


public class MzPeakReader
{
    IMZPeakArchiveStorage storage;

    SpectrumMetadataReader? spectrumMetadata;
    ChromatogramMetadataReader? chromatogramMetadata;

    DataArraysReaderMeta? spectrumArraysMeta = null;
    DataArraysReaderMeta? chromatogramArraysMeta = null;

    public MzPeakReader(string path) : this(new LocalZipArchive(path))
    {}

    public MzPeakReader(IMZPeakArchiveStorage storage)
    {
        this.storage = storage;
        var stream = storage.SpectrumMetadata();
        spectrumMetadata = stream == null ? null : new SpectrumMetadataReader(stream);
        stream = storage.ChromatogramMetadata();
        chromatogramMetadata = stream == null ? null : new ChromatogramMetadataReader(stream);
    }

    public int Length => spectrumMetadata?.Length ?? 0;
    public int SpectrumCount => spectrumMetadata?.Length ?? 0;
    public int ChromatogramCount => chromatogramMetadata?.Length ?? 0;
    public FileDescription FileDescription => spectrumMetadata?.FileDescription ?? chromatogramMetadata?.FileDescription ?? FileDescription.Empty();
    public List<InstrumentConfiguration> InstrumentConfigurations => spectrumMetadata?.InstrumentConfigurations ?? chromatogramMetadata?.InstrumentConfigurations ?? new();
    public List<Software> Softwares => spectrumMetadata?.Softwares ?? chromatogramMetadata?.Softwares ?? new();
    public List<Sample> Samples => spectrumMetadata?.Samples ?? chromatogramMetadata?.Samples ?? new();
    public List<DataProcessingMethod> DataProcessingMethods => spectrumMetadata?.DataProcessingMethods ?? chromatogramMetadata?.DataProcessingMethods ?? new();
    public MSRun Run => spectrumMetadata?.Run ?? chromatogramMetadata?.Run ?? new();
    public async Task<ChunkedArray?> GetChromatogramData(ulong index)
    {
        var dataFacet = storage.ChromatogramData();
        DataArraysReader reader;
        if (dataFacet == null)
        {
            return null;
        }
        if (chromatogramArraysMeta == null)
        {
            reader = new DataArraysReader(dataFacet, BufferContext.Chromatogram);
            chromatogramArraysMeta = reader.Metadata;
        }
        else
        {
            reader = new DataArraysReader(dataFacet, chromatogramArraysMeta);
        }
        return await reader.ReadForIndex(index);
    }

    public RecordBatch? SpectrumTable => spectrumMetadata?.SpectrumMetadata;

    public RecordBatch? ScanTable => spectrumMetadata?.ScanMetadata;

    public RecordBatch? PrecursorTable => spectrumMetadata?.PrecursorMetadata;

    public RecordBatch? SelectedIonTable => spectrumMetadata?.PrecursorMetadata;

    public RecordBatch? ChromatogramTable => chromatogramMetadata?.ChromatogramMetadata;

    public RecordBatch? ChromatogramPrecursorTable => chromatogramMetadata?.PrecursorMetadata;

    public RecordBatch? ChromatogramSelectedIonTable => chromatogramMetadata?.PrecursorMetadata;

    public SpectrumDescription GetSpectrumDescription(ulong index)
    {
        if (spectrumMetadata == null) throw new InvalidOperationException("Spectrum metadata table is absent");
        return spectrumMetadata.GetSpectrum(index);
    }

    public BufferFormat? SpectrumDataFormat { get
        {
            if (spectrumArraysMeta != null) return spectrumArraysMeta.Format;
            else
            {
                var reader = OpenSpectrumDataReader();
                if (reader == null) return null;
                return reader.Metadata.Format;
            }
        }
    }

    public DataArraysReaderMeta? SpectrumDataReaderMeta => OpenSpectrumDataReader()?.Metadata;

    public async IAsyncEnumerable<(SpectrumDescription, StructArray)> EnumerateSpectraAsync()
    {
        var reader = OpenSpectrumDataReader();
        if (reader != null)
        {
            var i = 0ul;
            var dataReader = reader;
            var it = dataReader.Enumerate();
            await foreach(var data in it)
            {
                var meta = GetSpectrumDescription(i);
                var item = (meta, data);
                yield return item;
                i += 1;
            }
        }
    }


    DataArraysReader? OpenSpectrumDataReader()
    {
        var dataFacet = storage.SpectrumData();
        DataArraysReader reader;
        if (dataFacet == null)
        {
            return null;
        }
        if (spectrumArraysMeta == null)
        {
            reader = new DataArraysReader(dataFacet, BufferContext.Spectrum);
            reader.SpacingModels = spectrumMetadata?.GetSpacingModelIndex();
            spectrumArraysMeta = reader.Metadata;
        }
        else
        {
            reader = new DataArraysReader(dataFacet, spectrumArraysMeta);
        }
        return reader;
    }

    public async Task<ChunkedArray?> GetSpectrumData(ulong index)
    {
        var reader = OpenSpectrumDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }
}