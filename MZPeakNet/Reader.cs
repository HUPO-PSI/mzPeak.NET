
using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Metadata;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;
using Microsoft.Extensions.Logging;

namespace MZPeak.Reader;

public class DataFacet<T>
{
    MetadataReaderBase<T> MetadataReader;
    DataArraysReader DataReader;


    public DataFacet(MetadataReaderBase<T> metadataReader, DataArraysReader dataReader)
    {
        MetadataReader = metadataReader;
        DataReader = dataReader;
    }

    public int Length => MetadataReader.Length;

    public async Task<(T, ChunkedArray)> Get(ulong index)
    {
        var meta = MetadataReader.Get(index);
        if (meta == null) throw new IndexOutOfRangeException();
        var data = await DataReader.ReadForIndex(index);
        if (data == null) throw new IndexOutOfRangeException();
        return (meta, data);
    }

    public async IAsyncEnumerable<(T, StructArray)> EnumerateAsync()
    {
        var metaRecs = MetadataReader.BulkLoad();
        var i = 0ul;
        await foreach (var (idx, data) in DataReader.Enumerate())
        {
            while (i < idx)
            {
                var metaSkipped = metaRecs[(int)i];
                yield return (metaSkipped, DataReader.EmptyArrays());
                i++;
            }
            var meta = metaRecs[(int)idx];

            var item = (meta, data);
            yield return item;
            i += 1;
        }
    }
}


public class MzPeakReader
{
    internal static ILogger? Logger = null;

    IMZPeakArchiveStorage storage;

    SpectrumMetadataReader? spectrumMetadata;
    ChromatogramMetadataReader? chromatogramMetadata;

    DataArraysReaderMeta? spectrumArraysMeta = null;
    DataArraysReaderMeta? chromatogramArraysMeta = null;
    DataArraysReaderMeta? spectrumPeaksArraysMeta = null;

    public MzPeakReader(string path) : this(new LocalZipArchive(path))
    { }

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
        return spectrumMetadata.Get(index);
    }

    public ChromatogramDescription GetChromatogramDescription(ulong index)
    {
        if (chromatogramMetadata == null) throw new InvalidOperationException("Chromatogram metadata table is absent");
        return chromatogramMetadata.Get(index);
    }

    public BufferFormat? SpectrumDataFormat
    {
        get
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

    public BufferFormat? ChromatogramDataFormat
    {
        get
        {
            if (chromatogramArraysMeta != null) return chromatogramArraysMeta.Format;
            else
            {
                var reader = OpenChromatogramDataReader();
                if (reader == null) return null;
                return reader.Metadata.Format;
            }
        }
    }

    public bool HasSpectrumPeaks => spectrumPeaksArraysMeta != null ? true : SpectrumPeaksDataReaderMeta != null;

    public DataArraysReaderMeta? SpectrumDataReaderMeta => OpenSpectrumDataReader()?.Metadata;
    public DataArraysReaderMeta? SpectrumPeaksDataReaderMeta => OpenSpectrumPeaksDataReader()?.Metadata;
    public DataArraysReaderMeta? ChromatogramDataReaderMeta => OpenChromatogramDataReader()?.Metadata;

    public async IAsyncEnumerable<(SpectrumDescription, StructArray)> EnumerateSpectraAsync()
    {
        var dataReader = OpenSpectrumDataReader();
        if (dataReader != null && spectrumMetadata != null)
        {
            await foreach (var item in new DataFacet<SpectrumDescription>(spectrumMetadata, dataReader).EnumerateAsync())
                yield return item;
        }
    }

    public async IAsyncEnumerable<(ChromatogramDescription, StructArray)> EnumerateChromatogramsAsync()
    {
        var dataReader = OpenChromatogramDataReader();
        if (dataReader != null && chromatogramMetadata != null)
        {
            await foreach (var item in new DataFacet<ChromatogramDescription>(chromatogramMetadata, dataReader).EnumerateAsync())
                yield return item;
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

    DataArraysReader? OpenSpectrumPeaksDataReader()
    {
        var dataFacet = storage.SpectrumPeaks();
        DataArraysReader reader;
        if (dataFacet == null)
        {
            return null;
        }
        if (spectrumPeaksArraysMeta == null)
        {
            reader = new DataArraysReader(dataFacet, BufferContext.Spectrum);
            reader.SpacingModels = spectrumMetadata?.GetSpacingModelIndex();
            spectrumPeaksArraysMeta = reader.Metadata;
        }
        else
        {
            reader = new DataArraysReader(dataFacet, spectrumPeaksArraysMeta);
        }
        return reader;
    }

    DataArraysReader? OpenChromatogramDataReader()
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
        return reader;
    }

    public async Task<ChunkedArray?> GetSpectrumData(ulong index)
    {
        var reader = OpenSpectrumDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }

    public async Task<ChunkedArray?> GetSpectrumPeaks(ulong index)
    {
        var reader = OpenSpectrumPeaksDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }

    public async Task<ChunkedArray?> GetChromatogramData(ulong index)
    {
        var reader = OpenChromatogramDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }
}