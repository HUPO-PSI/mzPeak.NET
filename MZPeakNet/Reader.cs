
using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Metadata;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;
using Microsoft.Extensions.Logging;

namespace MZPeak.Reader;

/// <summary>
/// Combines metadata and data array readers for unified access.
/// </summary>
/// <typeparam name="T">The metadata type (e.g., SpectrumDescription).</typeparam>
public class DataFacet<T> : IAsyncEnumerable<(T, StructArray)>
{
    MetadataReaderBase<T> MetadataReader;
    DataArraysReader DataReader;


    /// <summary>Creates a data facet combining metadata and data readers.</summary>
    /// <param name="metadataReader">The metadata reader.</param>
    /// <param name="dataReader">The data arrays reader.</param>
    public DataFacet(MetadataReaderBase<T> metadataReader, DataArraysReader dataReader)
    {
        MetadataReader = metadataReader;
        DataReader = dataReader;
    }

    /// <summary>Gets the number of entries in the facet.</summary>
    public int Length => MetadataReader.Length;

    /// <summary>Gets the metadata and data arrays for a specific index.</summary>
    /// <param name="index">The entry index.</param>
    public async Task<(T, StructArray)> Get(ulong index)
    {
        var meta = MetadataReader.Get(index);
        if (meta == null) throw new IndexOutOfRangeException();
        var data = await DataReader.ReadForIndex(index);
        if (data == null) throw new IndexOutOfRangeException();
        return (meta, data);
    }

    /// <summary>Asynchronously enumerates all entries with their metadata and data.</summary>
    public async IAsyncEnumerable<(T, StructArray)> EnumerateAsync()
    {
        var metaRecs = MetadataReader.BulkLoad();
        var i = 0ul;
        await foreach (var (idx, data) in DataReader)
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

    public async IAsyncEnumerator<(T, StructArray)> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await foreach(var x in EnumerateAsync())
        {
            yield return x;
        }
    }
}


/// <summary>
/// Reader for mzPeak archive files containing mass spectrometry data.
/// </summary>
public class MzPeakReader
{
    internal static ILogger? Logger = null;

    IMZPeakArchiveStorage storage;

    SpectrumMetadataReader? spectrumMetadata;
    ChromatogramMetadataReader? chromatogramMetadata;
    SpectrumMetadataReader? wavelengthSpectrumMetadata;

    DataArraysReaderMeta? spectrumArraysMeta = null;
    DataArraysReaderMeta? chromatogramArraysMeta = null;
    DataArraysReaderMeta? spectrumPeaksArraysMeta = null;
    DataArraysReaderMeta? wavelengthSpectrumArraysMeta = null;

    /// <summary>Creates a reader for the mzPeak file at the specified path.</summary>
    /// <param name="path">The file path to the mzPeak archive.</param>
    public MzPeakReader(string path) : this(new LocalZipArchive(path))
    { }

    /// <summary>Creates a reader using the specified storage backend.</summary>
    /// <param name="storage">The archive storage implementation.</param>
    public MzPeakReader(IMZPeakArchiveStorage storage)
    {
        this.storage = storage;
        var stream = storage.SpectrumMetadata();
        spectrumMetadata = stream == null ? null : new SpectrumMetadataReader(stream);
        stream = storage.ChromatogramMetadata();
        chromatogramMetadata = stream == null ? null : new ChromatogramMetadataReader(stream);
        stream = storage.WavelengthSpectrumMetadata();
        wavelengthSpectrumMetadata = stream == null ? null : new SpectrumMetadataReader(stream);
    }

    /// <summary>Gets the number of spectra (alias for SpectrumCount).</summary>
    public int Length => spectrumMetadata?.Length ?? 0;
    /// <summary>Gets the number of spectra in the file.</summary>
    public int SpectrumCount => spectrumMetadata?.Length ?? 0;
    /// <summary>Gets the number of chromatograms in the file.</summary>
    public int ChromatogramCount => chromatogramMetadata?.Length ?? 0;
    /// <summary>Gets the number of wavelength spectra in the file.</summary>
    public int WavelengthSpectrumCount => wavelengthSpectrumMetadata?.Length ?? 0;

    public bool HasSpectrumData => spectrumMetadata != null;
    public bool HasChromatogramData => chromatogramMetadata != null;
    public bool HasWavelengthData => wavelengthSpectrumMetadata != null;

    /// <summary>Gets the file description metadata.</summary>
    public FileDescription FileDescription => spectrumMetadata?.FileDescription ?? chromatogramMetadata?.FileDescription ?? FileDescription.Empty();
    /// <summary>Gets the list of instrument configurations.</summary>
    public List<InstrumentConfiguration> InstrumentConfigurations => spectrumMetadata?.InstrumentConfigurations ?? chromatogramMetadata?.InstrumentConfigurations ?? new();
    /// <summary>Gets the list of software used.</summary>
    public List<Software> Softwares => spectrumMetadata?.Softwares ?? chromatogramMetadata?.Softwares ?? new();
    /// <summary>Gets the list of samples.</summary>
    public List<Sample> Samples => spectrumMetadata?.Samples ?? chromatogramMetadata?.Samples ?? new();
    /// <summary>Gets the list of data processing methods.</summary>
    public List<DataProcessingMethod> DataProcessingMethods => spectrumMetadata?.DataProcessingMethods ?? chromatogramMetadata?.DataProcessingMethods ?? new();
    /// <summary>Gets the run-level metadata.</summary>
    public MSRun Run => spectrumMetadata?.Run ?? chromatogramMetadata?.Run ?? new();

    /// <summary>Gets the spectrum metadata as an Arrow ChunkedArray.</summary>
    public ChunkedArray? SpectrumTable => spectrumMetadata?.SpectrumMetadata;

    /// <summary>Gets the scan metadata as an Arrow ChunkedArray.</summary>
    public ChunkedArray? ScanTable => spectrumMetadata?.ScanMetadata;

    /// <summary>Gets the precursor metadata as an Arrow ChunkedArray.</summary>
    public ChunkedArray? PrecursorTable => spectrumMetadata?.PrecursorMetadata;

    /// <summary>Gets the selected ion metadata as an Arrow ChunkedArray.</summary>
    public ChunkedArray? SelectedIonTable => spectrumMetadata?.PrecursorMetadata;

    /// <summary>Gets the chromatogram metadata as an Arrow ChunkedArray.</summary>
    public ChunkedArray? ChromatogramTable => chromatogramMetadata?.ChromatogramMetadata;

    /// <summary>Gets the chromatogram precursor metadata as an Arrow ChunkedArray.</summary>
    public ChunkedArray? ChromatogramPrecursorTable => chromatogramMetadata?.PrecursorMetadata;

    /// <summary>Gets the chromatogram selected ion metadata as an Arrow ChunkedArray.</summary>
    public ChunkedArray? ChromatogramSelectedIonTable => chromatogramMetadata?.PrecursorMetadata;

    public ChunkedArray? WavelengthSpectrumTable => wavelengthSpectrumMetadata?.SpectrumMetadata;
    public ChunkedArray? WavelengthSpectrumScanTable => wavelengthSpectrumMetadata?.ScanMetadata;

    /// <summary>Gets the spectrum description for the specified index.</summary>
    /// <param name="index">The spectrum index.</param>
    public SpectrumDescription GetSpectrumDescription(ulong index)
    {
        if (spectrumMetadata == null) throw new InvalidOperationException("Spectrum metadata table is absent");
        return spectrumMetadata.Get(index);
    }

    /// <summary>Gets the chromatogram description for the specified index.</summary>
    /// <param name="index">The chromatogram index.</param>
    public ChromatogramDescription GetChromatogramDescription(ulong index)
    {
        if (chromatogramMetadata == null) throw new InvalidOperationException("Chromatogram metadata table is absent");
        return chromatogramMetadata.Get(index);
    }

    /// <summary>Gets the spectrum description for the specified index.</summary>
    /// <param name="index">The spectrum index.</param>
    public SpectrumDescription GetWavelengthSpectrumDescription(ulong index)
    {
        if (wavelengthSpectrumMetadata == null) throw new InvalidOperationException("Wavelength spectrum metadata table is absent");
        return wavelengthSpectrumMetadata.Get(index);
    }

    /// <summary>Gets the buffer format used for spectrum data arrays.</summary>
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

    /// <summary>Gets the buffer format used for chromatogram data arrays.</summary>
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

    /// <summary>Gets the buffer format used for chromatogram data arrays.</summary>
    public BufferFormat? WavelengthSpectrumDataFormat
    {
        get
        {
            if (wavelengthSpectrumArraysMeta != null) return wavelengthSpectrumArraysMeta.Format;
            else
            {
                var reader = OpenWavelengthSpectrumDataReader();
                if (reader == null) return null;
                return reader.Metadata.Format;
            }
        }
    }

    /// <summary>Gets whether the file contains spectrum peak data.</summary>
    public bool HasSpectrumPeaks => spectrumPeaksArraysMeta != null ? true : SpectrumPeaksDataReaderMeta != null;

    /// <summary>Gets the metadata for the spectrum data reader.</summary>
    public DataArraysReaderMeta? SpectrumDataReaderMeta => OpenSpectrumDataReader()?.Metadata;
    /// <summary>Gets the metadata for the spectrum peaks data reader.</summary>
    public DataArraysReaderMeta? SpectrumPeaksDataReaderMeta => OpenSpectrumPeaksDataReader()?.Metadata;
    /// <summary>Gets the metadata for the chromatogram data reader.</summary>
    public DataArraysReaderMeta? ChromatogramDataReaderMeta => OpenChromatogramDataReader()?.Metadata;

    /// <summary>Asynchronously enumerates all spectra with their descriptions and data.</summary>
    public async IAsyncEnumerable<(SpectrumDescription, StructArray)> EnumerateSpectraAsync()
    {
        var dataReader = OpenSpectrumDataReader();
        if (dataReader != null && spectrumMetadata != null)
        {
            await foreach (var item in new DataFacet<SpectrumDescription>(spectrumMetadata, dataReader).EnumerateAsync())
                yield return item;
        }
    }

    /// <summary>Asynchronously enumerates all chromatograms with their descriptions and data.</summary>
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
            reader = new DataArraysReader(dataFacet, BufferContext.Spectrum)
            {
                SpacingModels = spectrumMetadata?.GetSpacingModelIndex()
            };
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

    DataArraysReader? OpenWavelengthSpectrumDataReader()
    {
        var dataFacet = storage.WavelengthSpectrumData();
        DataArraysReader reader;
        if (dataFacet == null)
        {
            return null;
        }
        if (wavelengthSpectrumArraysMeta == null)
        {
            reader = new DataArraysReader(dataFacet, BufferContext.WavelengthSpectrum);
            wavelengthSpectrumArraysMeta = reader.Metadata;
        }
        else
        {
            reader = new DataArraysReader(dataFacet, wavelengthSpectrumArraysMeta);
        }
        return reader;
    }

    /// <summary>Gets the data arrays for a spectrum by index.</summary>
    /// <param name="index">The spectrum index.</param>
    public async Task<StructArray?> GetSpectrumData(ulong index)
    {
        var reader = OpenSpectrumDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }

    /// <summary>Gets the data arrays for a wavelength spectrum by index.</summary>
    /// <param name="index">The spectrum index.</param>
    public async Task<StructArray?> GetWavelengthSpectrumData(ulong index)
    {
        var reader = OpenWavelengthSpectrumDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }


    /// <summary>Gets the peak data arrays for a spectrum by index.</summary>
    /// <param name="index">The spectrum index.</param>
    public async Task<StructArray?> GetSpectrumPeaks(ulong index)
    {
        var reader = OpenSpectrumPeaksDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }

    /// <summary>Gets the data arrays for a chromatogram by index.</summary>
    /// <param name="index">The chromatogram index.</param>
    public async Task<StructArray?> GetChromatogramData(ulong index)
    {
        var reader = OpenChromatogramDataReader();
        if (reader == null) return null;
        return await reader.ReadForIndex(index);
    }
}