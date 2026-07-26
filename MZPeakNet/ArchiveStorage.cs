namespace MZPeak.Storage;

using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.IO.Compression;

using ParquetSharp.IO;
using ParquetSharp.Encryption;
using System.Text;
using Microsoft.Extensions.Logging;

using ParquetSharp;
using ParquetSharp.Arrow;
using DecryptionConfigurations = Dictionary<string, ParquetSharp.FileDecryptionProperties>;
using MZPeak.ControlledVocabulary;
using System.Net.Http.Headers;
using System.Threading;
using System.IO.MemoryMappedFiles;

#region Index File Implementation

public enum EntityTypeTag
{
    Spectrum,
    Chromatogram,
    WavelengthSpectrum,
    Other
}

[JsonConverter(typeof(EntityTypeJsonConverter))]
public record struct EntityType(EntityTypeTag Tag, string? Value) : IComparable<EntityTypeTag>
{
    public int CompareTo(EntityTypeTag other)
    {
        return Tag.CompareTo(other);
    }

    public static EntityType Spectrum => new(EntityTypeTag.Spectrum, null);
    public static EntityType Chromatogram => new(EntityTypeTag.Chromatogram, null);
    public static EntityType WavelengthSpectrum => new(EntityTypeTag.WavelengthSpectrum, null);
}


class EntityTypeJsonConverter : JsonConverter<EntityType>
{
    public override EntityType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
        {
            throw new JsonException();
        }

        var val = reader.GetString()?.ToLower();
        if (val == null) throw new JsonException("Entity type JSON cannot be null");
        return val switch
        {
            "spectrum" => new EntityType(EntityTypeTag.Spectrum, null),
            "chromatogram" => new EntityType(EntityTypeTag.Chromatogram, null),
            "wavelength spectrum" => new EntityType(EntityTypeTag.WavelengthSpectrum, null),
            _ => new EntityType(EntityTypeTag.Other, val)
        };
    }

    public override void Write(Utf8JsonWriter writer, EntityType value, JsonSerializerOptions options)
    {
        if (value.Tag == EntityTypeTag.Other) {
            writer.WriteStringValue(value.Value);
        } else
        {
            var text = value.Tag switch
            {
                EntityTypeTag.Spectrum => "spectrum",
                EntityTypeTag.Chromatogram => "chromatogram",
                EntityTypeTag.WavelengthSpectrum => "wavelength spectrum",
                _ => throw new NotImplementedException()
            };
            writer.WriteStringValue(text);
        }

    }
}


public enum DataKindTag
{
    DataArrays,
    Metadata,
    Scans,
    Precursors,
    SelectedIons,
    Products,
    Peaks,
    Other,
    Proprietary
}


[JsonConverter(typeof(DataKindTJsonConverter))]
public record struct DataKind(DataKindTag Tag, string? Value) : IComparable<DataKindTag>
{
    public int CompareTo(DataKindTag other)
    {
        return Tag.CompareTo(other);
    }

    public static DataKind DataArrays => new(DataKindTag.DataArrays, null);
    public static DataKind Metadata => new(DataKindTag.Metadata, null);
    public static DataKind Peaks => new(DataKindTag.Peaks, null);
    public static DataKind Proprietary => new(DataKindTag.Proprietary, null);
    public static DataKind Scans => new(DataKindTag.Scans, null);
    public static DataKind Precursors => new(DataKindTag.Precursors, null);
    public static DataKind SelectedIons => new(DataKindTag.SelectedIons, null);
    public static DataKind Products => new(DataKindTag.Products, null);
}


class DataKindTJsonConverter : JsonConverter<DataKind>
{
    public override DataKind Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
        {
            throw new JsonException();
        }

        var val = reader.GetString()?.ToLower();
        if (val == null) throw new JsonException("Data kind JSON cannot be null");
        return val switch
        {
            "data arrays" => new(DataKindTag.DataArrays, null),
            "data_arrays" => new(DataKindTag.DataArrays, null),
            "metadata" => new(DataKindTag.Metadata, null),
            "scans" => new(DataKindTag.Scans, null),
            "precursors" => new(DataKindTag.Precursors, null),
            "selected_ions" => new(DataKindTag.SelectedIons, null),
            "products" => new(DataKindTag.Products, null),
            "peaks" => new(DataKindTag.Peaks, null),
            "proprietary" => new(DataKindTag.Proprietary, null),
            _ => new(DataKindTag.Other, val)
        };
    }

    public override void Write(Utf8JsonWriter writer, DataKind value, JsonSerializerOptions options)
    {
        if (value.Tag == DataKindTag.Other)
        {
            writer.WriteStringValue(value.Value);
        }
        else
        {
            var text = value.Tag switch
            {
                DataKindTag.DataArrays => "data_arrays",
                DataKindTag.Metadata => "metadata",
                DataKindTag.Scans => "scans",
                DataKindTag.Precursors => "precursors",
                DataKindTag.SelectedIons => "selected_ions",
                DataKindTag.Products => "products",
                DataKindTag.Peaks => "peaks",
                DataKindTag.Proprietary => "proprietary",
                _ => throw new NotImplementedException()
            };
            writer.WriteStringValue(text);
        }

    }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record class ColumnMapping
{
    [JsonPropertyName("name")]
    public string Name {get; set;}

    [JsonPropertyName("path")]
    public List<string> Path {get; set;}

    [JsonPropertyName("accession")]
    public string? Accession {get; set;}

    [JsonPropertyName("unit")]
    public string? Unit {get; set;}

    [JsonIgnore]
    public string? CURIE => Accession;
    [JsonIgnore]
    public string? UnitCURIE => Unit;

    public string Leaf() => Path.Last();

    public Param Create(object? rawValue = null) => new Param(Name, Accession, rawValue, Unit);

    public ColumnMapping(string name, List<string> path, string? accession, string? unit)
    {
        Name = name;
        Path = path;
        Accession = accession;
        Unit = unit;
    }

    public override string ToString()
    {
        return $"ColumnMapping {{ Name = {Name}, Path = [{string.Join(',', Path)}], Accession = {Accession}, Unit = {Unit} }}";
    }
}


[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record FileIndexEntry
{
    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("entity_type")]
    public EntityType EntityType { get; set; }

    [JsonPropertyName("data_kind")]
    public DataKind DataKind { get; set; }

    [JsonPropertyName("parameters")]
    public List<Param> Params { get; set; }

    [JsonPropertyName("column_mapping")]
    public List<ColumnMapping> ColumnMappings {get; set;}

    public static FileIndexEntry FromEntityAndData(EntityType entityType, DataKind dataKind, List<Param>? @params=null, List<ColumnMapping>? columnMappings = null)
    {
        string entityTypeTag = "";
        switch (entityType.Tag)
        {
            case EntityTypeTag.Chromatogram:
                {
                    entityTypeTag = "chromatograms";
                    break;
                }
            case EntityTypeTag.Spectrum:
                {
                    entityTypeTag = "spectra";
                    break;
                }
            case EntityTypeTag.WavelengthSpectrum:
                {
                    entityTypeTag = "wavelength_spectra";
                    break;
                }
            case EntityTypeTag.Other:
                {
                    throw new NotImplementedException(entityType.ToString());
                }
        }
        string dataKindTag = "";
        switch (dataKind.Tag)
        {
            case DataKindTag.DataArrays:
                {
                    dataKindTag = "data";
                    break;
                }
            case DataKindTag.Metadata:
                {
                    dataKindTag = "metadata";
                    break;
                }
            case DataKindTag.Scans:
                {
                    dataKindTag = "metadata_scans";
                    break;
                }
            case DataKindTag.Precursors:
                {
                    dataKindTag = "metadata_precursors";
                    break;
                }
            case DataKindTag.SelectedIons:
                {
                    dataKindTag = "metadata_selected_ions";
                    break;
                }
            case DataKindTag.Products:
                {
                    dataKindTag = "metadata_products";
                    break;
                }
            case DataKindTag.Peaks:
                {
                    dataKindTag = "peaks";
                    break;
                }
            case DataKindTag.Proprietary:
                {
                    dataKindTag = dataKind.Value ?? dataKind.Tag.ToString();
                    break;
                }
            case DataKindTag.Other:
                {
                    dataKindTag = dataKind.Value ?? dataKind.Tag.ToString();
                    break;
                }
        }
        return new FileIndexEntry(
            string.Format("{0}_{1}.parquet", entityTypeTag, dataKindTag),
            entityType,
            dataKind,
            @params ?? [],
            columnMappings ?? []
        );
    }

    public FileIndexEntry(string name, EntityType entityType, DataKind dataKind, List<Param>? @params=null, List<ColumnMapping>? columnMappings=null)
    {
        Name = name;
        EntityType = entityType;
        DataKind = dataKind;
        Params = @params ?? [];
        ColumnMappings = columnMappings ?? [];
    }

    public override string ToString()
    {
        return $@"FileIndexEntry({Name}, {EntityType}, {DataKind}, [{string.Join(", ", Params.Select(p => p.ToString()))}], [{string.Join(", ", ColumnMappings.Select(e => e.ToString()))}])";
    }
}


[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public class FileIndex
{
    public const string FILE_NAME = "mzpeak_index.json";

    [JsonPropertyName("files")]
    public List<FileIndexEntry> Files { get; set; }

    [JsonPropertyName("metadata")]
    public JsonObject Metadata { get; set; }

    public FileIndexEntry? FindEntry(EntityType entityType, DataKind dataKind)
    {
        foreach(var entry in Files)
        {
            if (entry.DataKind == dataKind && entry.EntityType == entityType)
                return entry;
        }
        return null;
    }

    public static DecryptionConfigurations UniformDecryption(FileDecryptionProperties decryptionProperties)
    {
        DecryptionConfigurations decryptionConfigs = new();
        List<DataKind> dataKinds = [
            DataKind.DataArrays,
            DataKind.Peaks,
            DataKind.Metadata,
            DataKind.Scans,
            DataKind.Precursors,
            DataKind.SelectedIons,
            DataKind.Products,
            DataKind.Proprietary,
        ];
        List<EntityType> entityTypes = [
            EntityType.Spectrum,
            EntityType.Chromatogram,
            EntityType.WavelengthSpectrum,
        ];
        foreach(var e in entityTypes)
        {
            foreach(var d in dataKinds)
            {
                decryptionConfigs[FileIndexEntry.FromEntityAndData(e, d).Name] = decryptionProperties;
            }
        }
        return decryptionConfigs;
    }

    public FileIndex()
    {
        Files = new List<FileIndexEntry>();
        Metadata = new JsonObject();
    }
}


public class MzPeakFacetNamespace
{
    public EntityType EntityType { get; set; }
    public IMZPeakArchiveStorage Storage {get; set;}

    public override string ToString()
    {
        return $"MzPeakFacetNamespace({EntityType} with {FileIndex.Files.Count(e => e.EntityType == EntityType)} files from {Storage})";
    }

    public FileIndex FileIndex => Storage.FileIndex();

    public MzPeakFacetNamespace(EntityType entityType, IMZPeakArchiveStorage storage)
    {
        EntityType = entityType;
        Storage = storage;
    }

    public FileReader? OpenDataKind(DataKind dataKind, ReaderProperties? props=null, ArrowReaderProperties? arrowProps=null)
    {
        var entry = FileIndex.FindEntry(EntityType, dataKind);
        if (entry == null) return null;
        return Storage.OpenFromFileIndexEntry(entry, props, arrowProps);
    }

    public bool Has(DataKind dataKind) => FileIndex.FindEntry(EntityType, dataKind) != null;

    public FileIndexEntry? FindEntry(DataKind dataKind) => FileIndex.FindEntry(EntityType, dataKind);

    public List<ColumnMapping>? ColumnMappings(DataKind dataKind) => FileIndex.FindEntry(EntityType, dataKind)?.ColumnMappings;

    public FileReader? OpenMetadata(ReaderProperties? props = null, ArrowReaderProperties? arrowProps = null) => OpenDataKind(DataKind.Metadata, props, arrowProps);
    public FileReader? OpenScans(ReaderProperties? props = null, ArrowReaderProperties? arrowProps = null) => OpenDataKind(DataKind.Scans, props, arrowProps);
    public FileReader? OpenPrecursors(ReaderProperties? props = null, ArrowReaderProperties? arrowProps = null) => OpenDataKind(DataKind.Precursors, props, arrowProps);
    public FileReader? OpenSelectedIons(ReaderProperties? props = null, ArrowReaderProperties? arrowProps = null) => OpenDataKind(DataKind.SelectedIons, props, arrowProps);
    public FileReader? OpenProducts(ReaderProperties? props = null, ArrowReaderProperties? arrowProps = null) => OpenDataKind(DataKind.Products, props, arrowProps);
}

#endregion

/// <summary>
/// A common interface for reading mzPeak archives from different types of underlying storage
/// including local ZIP archives and directories, in-memory ZIP archives, or remote ZIP archives
/// accessed over HTTP(S).
/// </summary>
public interface IMZPeakArchiveStorage : IDisposable
{
    internal static ILogger? Logger = null;

    public DecryptionConfigurations DecryptionConfigurations { get; set; }

    /// <summary>
    /// Get the list of file names in the archive. This may include files not in the index.
    /// </summary>
    /// <returns></returns>
    public List<string> FileNames();

    /// <summary>
    /// Open the archive member corresponding to `entityType` and `dataKind`, if one exists.
    ///
    /// If multiple matches exist, only the first is returned.
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="dataKind"></param>
    /// <returns></returns>
    public Stream? OpenEntry(EntityType entityType, DataKind dataKind)
    {
        var entry = FileIndex().FindEntry(entityType, dataKind);
        if (entry == null)
        {
            return null;
        }
        else
        {
            return OpenStream(entry.Name);
        }
    }

    public MzPeakFacetNamespace? OpenNamespace(EntityType entityType)
    {
        var ns = new MzPeakFacetNamespace(entityType, this);
        return ns.Has(DataKind.Metadata) ? ns : null;
    }

    public FileReader? OpenFromFileIndexEntry(FileIndexEntry entry, ReaderProperties? props=null, ArrowReaderProperties? arrowProps=null)
    {
        if (props == null)
            props = ReaderProperties.GetDefaultReaderProperties();
        if (arrowProps == null)
            arrowProps = ArrowReaderProperties.GetDefault();
        if (entry == null) return null;
        var stream = OpenStream(entry.Name);
        Logger?.LogTrace("Opening {entry}", entry);
        if (DecryptionConfigurations.ContainsKey(entry.Name))
        {
            Logger?.LogTrace("{entry} has decryption config", entry);
            props.FileDecryptionProperties = DecryptionConfigurations[entry.Name];
        }
        return stream == null ? null : new FileReader(
            new ManagedRandomAccessFile(stream),
            props,
            arrowProps
        );
    }

    /// <summary>
    /// Open the spectrum data arrays volume, if it exists, null otherwise.
    /// </summary>
    /// <returns></returns>
    public FileReader? SpectrumData(long bufferSize = 4096 * 4, bool prebuffer = false)
    {
        var entry = FileIndex().FindEntry(EntityType.Spectrum, DataKind.DataArrays);
        var arrowProps = ArrowReaderProperties.GetDefault();
        arrowProps.BatchSize = bufferSize;
        arrowProps.PreBuffer = prebuffer;
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, arrowProps);
    }

    /// <summary>
    /// Open the spectrum data arrays volume containing explicitly centroided peaks, if it exists, null otherwise.
    /// </summary>
    /// <returns></returns>
    public FileReader? SpectrumPeaks(long bufferSize = 4096 * 4, bool prebuffer = false)
    {
        var entry = FileIndex().FindEntry(EntityType.Spectrum, DataKind.Peaks);
        var arrowProps = ArrowReaderProperties.GetDefault();
        arrowProps.BatchSize = bufferSize;
        arrowProps.PreBuffer = prebuffer;
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, arrowProps);
    }

    /// <summary>
    /// Open the chromatogram data arrays volume, if it exists, null otherwise.
    /// </summary>
    /// <returns></returns>
    public FileReader? ChromatogramData(long bufferSize = 4096)
    {
        var entry = FileIndex().FindEntry(EntityType.Chromatogram, DataKind.DataArrays);
        var arrowProps = ArrowReaderProperties.GetDefault();
        arrowProps.BatchSize = bufferSize;
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, arrowProps);
    }

    /// <summary>
    /// Open the spectrum metadata volume, if it exists, null otherwise.
    /// </summary>
    /// <returns></returns>
    public FileReader? SpectrumMetadata()
    {
        var entry = FileIndex().FindEntry(EntityType.Spectrum, DataKind.Metadata);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, null);
    }

    public FileReader? SpectrumMetadataScans()
    {
        var entry = FileIndex().FindEntry(EntityType.Spectrum, DataKind.Scans);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, null);
    }

    public FileReader? SpectrumMetadataPrecursors()
    {
        var entry = FileIndex().FindEntry(EntityType.Spectrum, DataKind.Precursors);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, null);
    }

    public FileReader? SpectrumMetadataSelectedIons()
    {
        var entry = FileIndex().FindEntry(EntityType.Spectrum, DataKind.SelectedIons);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, null);
    }

    /// <summary>
    /// Open the chromatogram metadata volume, if it exists, null otherwise.
    /// </summary>
    /// <returns></returns>
    public FileReader? ChromatogramMetadata()
    {
        var entry = FileIndex().FindEntry(EntityType.Chromatogram, DataKind.Metadata);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry);
    }

    public FileReader? ChromatogramMetadataPrecursors()
    {
        var entry = FileIndex().FindEntry(EntityType.Chromatogram, DataKind.Precursors);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, null);
    }

    public FileReader? ChromatogramMetadataSelectedIons()
    {
        var entry = FileIndex().FindEntry(EntityType.Chromatogram, DataKind.SelectedIons);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, null);
    }

    public FileReader? ChromatogramMetadataProducts()
    {
        var entry = FileIndex().FindEntry(EntityType.Chromatogram, DataKind.Products);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry, null, null);
    }

    /// <summary>
    /// Open the wavelength spectrum metadata volume, if it exists, null otherwise.
    /// </summary>
    /// <returns></returns>
    public FileReader? WavelengthSpectrumMetadata()
    {
        var entry = FileIndex().FindEntry(EntityType.WavelengthSpectrum, DataKind.Metadata);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry);
    }

    /// <summary>
    /// Open the wavelength spectrum data arrays volume, if it exists, null otherwise.
    /// </summary>
    /// <returns></returns>
    public FileReader? WavelengthSpectrumData()
    {
        var entry = FileIndex().FindEntry(EntityType.WavelengthSpectrum, DataKind.DataArrays);
        if (entry == null) return null;
        return OpenFromFileIndexEntry(entry);
    }

    /// <summary>
    /// Open the requested file name in the archive
    /// </summary>
    /// <param name="name">The file name to open</param>
    /// <returns>Readable, seekable stream</returns>
    public Stream OpenStream(string name);

    /// <summary>
    /// Access the file index from the archive
    /// </summary>
    /// <returns></returns>
    public FileIndex FileIndex();
}


/// <summary>
/// A facade around a single Stream that spans only a byte range
/// </summary>
public class StreamSegment : Stream, IDisposable
{
    /// <summary>
    /// The underlying stream to read from
    /// </summary>
    Stream Stream;

    /// <summary>
    /// The offset in the underlying stream to the segment's first byte
    /// </summary>
    long Offset;

    /// <summary>
    /// The length of the segment in bytes
    /// </summary>
    long SegmentLength;

    /// <summary>
    /// Whether or not to leave the underlying stream open when this stream is closed
    /// </summary>
    bool LeaveOpen;

    /// <summary>
    /// Create a new Stream object that spans a segment of an existing Stream.
    /// </summary>
    /// <param name="stream">The stream to read from</param>
    /// <param name="offset">The offset to the 0th byte in the segment</param>
    /// <param name="length">The length of the segment in bytes</param>
    /// <param name="leaveOpen">Whether or not to leave the underlying stream open when this stream is closed</param>
    public StreamSegment(Stream stream, long offset, long length, bool leaveOpen = false)
    {
        Stream = stream;
        Offset = offset;
        SegmentLength = length;
        LeaveOpen = leaveOpen;
        Configure();
    }

    void IDisposable.Dispose()
    {
        if (!LeaveOpen)
        {
            Stream.Dispose();
        }
    }

    public override bool CanRead => true;

    public override bool CanSeek => true;

    public override bool CanWrite => false;

    public override long Length => SegmentLength;

    public override long Position
    {
        get => Stream.Position - Offset;
        set => Stream.Position = Offset + value;
    }

    public override void Flush() => Stream.Flush();

    public override int Read(byte[] buffer, int offset, int count)
    {
        long bytesToRead = count - offset;
        if (Position + bytesToRead > SegmentLength)
        {
            bytesToRead = SegmentLength - Position;
        }
        return Stream.Read(buffer, offset, (int)bytesToRead);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        switch (origin)
        {
            case SeekOrigin.Begin:
                {
                    Position = offset < SegmentLength ? offset : SegmentLength;
                    break;
                }
            case SeekOrigin.Current:
                {
                    Position = Position + offset < SegmentLength ? Position + offset : SegmentLength;
                    break;
                }
            case SeekOrigin.End:
                {
                    throw new NotImplementedException();
                }
        }
        return Position;
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }

    public void Configure()
    {
        Stream.Seek(Offset, SeekOrigin.Begin);
    }
}


#region ZIP Archive Reading

/// <summary>
/// A base class providing common methods for mzPeak ZIP archive reading.
/// </summary>
public abstract class BaseZipArchive : IMZPeakArchiveStorage
{
    static int ZIP_LEADER = 0x04034b50;

    protected List<string> fileNames;
    protected FileIndex fileIndex;

    public DecryptionConfigurations DecryptionConfigurations { get; set; }

    /// <summary>
    /// Check if the provided bytes matches the ZIP magic bytes
    /// </summary>
    /// <param name="data">Bytes to compare</param>
    /// <returns>Whether the bytes imply this is a ZIP archive</returns>
    public static bool IsZipArchiveHeader(byte[] data)
    {
        if (data == null || data.Length < 4) return false;
        return BitConverter.ToInt32(data, 0) == ZIP_LEADER;
    }

    /// <summary>
    /// Check if this stream is a ZIP archive by checking if it starts with ZIP magic bytes
    /// </summary>
    /// <param name="stream"></param>
    /// <returns></returns>
    public static bool IsStreamZip(Stream stream)
    {
        long? pos = null;
        if (stream.CanSeek)
        {
            pos = stream.Position;
        }

        byte[] buf = [0, 0, 0, 0];
        stream.ReadExactly(buf);
        if (stream.CanSeek && pos != null)
        {
            stream.Position = pos.Value;
        }
        return IsZipArchiveHeader(buf);
    }

    public BaseZipArchive(DecryptionConfigurations? decryptionConfigurations = null)
    {
        fileNames = new List<string>();
        fileIndex = new FileIndex();
        DecryptionConfigurations = decryptionConfigurations ?? new();
    }

    public List<string> FileNames()
    {
        return fileNames;
    }

    public FileIndex FileIndex()
    {
        return fileIndex;
    }

    /// <summary>
    /// Open the main archive stream which will be used to access the contained entry files.
    /// </summary>
    /// <returns></returns>
    public abstract Stream OpenArchiveStream();

    /// <summary>
    /// Get a `ZipArchive` instance handle wrapping the entire archive stream
    /// </summary>
    /// <returns></returns>
    public virtual ZipArchive OpenArchive()
    {
        var stream = OpenArchiveStream();
        return new ZipArchive(stream, ZipArchiveMode.Read);
    }

    /// <summary>
    /// Open a specific file entry in the ZIP archive
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public abstract Stream OpenStream(string name);

    /// <summary>
    /// Find and load the mzPeak file index JSON from the ZIP archive, as well as enumerating
    /// the files in the archive to match up with the index contents.
    /// </summary>
    /// <exception cref="InvalidDataException">If the index file was not successfully parsed</exception>
    /// <exception cref="FileNotFoundException">If the index file is not found</exception>
    protected void ExtractInitialMetadata()
    {
        List<string> fileNames = [];
        FileIndex? fileIndex = null;
        using (var archive = OpenArchive())
        {
            foreach (var entry in archive.Entries)
            {
                fileNames.Add(entry.Name);
                if (entry.Name == Storage.FileIndex.FILE_NAME)
                {
                    using (var stream = new StreamReader(entry.Open()))
                    {
                        var indexJson = stream.ReadToEnd();
                        fileIndex = JsonSerializer.Deserialize<FileIndex>(indexJson);
                        if (fileIndex == null) throw new InvalidDataException($"Index JSON file did not deserialize successfully from {indexJson}");
                    }
                }
            }
        }
        this.fileNames = fileNames;
        if (fileIndex == null)
            throw new FileNotFoundException("Index JSON file not found");
        this.fileIndex = fileIndex;
    }

    /// <summary>
    /// A method to open an archive entry as a SegmentStream, assuming the underlying ZIP archive
    /// source supports having multiple independent cursors e.g. like a file on disk opened for reading
    /// multiple times.
    /// </summary>
    /// <param name="name">The name of the ZIP entry to open</param>
    /// <returns></returns>
    /// <exception cref="FileNotFoundException">When the provided name is not found</exception>
    /// <exception cref="InvalidDataException">When the ZIP entry is compressed. Only uncompressed members are supported</exception>
    protected virtual Stream OpenStreamIsolated(string name)
    {
        Stream stream;
        long length = 0;
        long offset = 0;
        using (stream = OpenArchiveStream())
        {

            var archive = new ZipArchive(stream, ZipArchiveMode.Read);
            var entry = archive.GetEntry(name);
            if (entry == null)
                throw new FileNotFoundException(name);

            // Hacky means of checking that the file isn't compressed since the actual compression
            // method isn't exposed by the ZipArchiveEntry API
            if (entry.Length != entry.CompressedLength)
                throw new InvalidDataException("File in MZPeak ZIP Archive cannot be stored with compression");

            length = entry.Length;
            // Hacky means of getting the offset of the file contents since it isn't exposed either
            using (var substreamNotSeekable = entry.Open())
                offset = stream.Position;
        }
        stream = OpenArchiveStream();
        var segStream = new StreamSegment(stream, offset, length);
        return segStream;
    }

    /// <summary>
    /// A method to open an archive entry as a SegmentStream, re-using the archive's stream. This implies that
    /// there may not be more than one opened entry at a time. This is needed when the archive cannot be re-opened
    /// like when wrapping a MemoryStream.
    /// </summary>
    /// <param name="name">The name of the ZIP entry to open</param>
    /// <returns></returns>
    /// <exception cref="FileNotFoundException">When the provided name is not found</exception>
    /// <exception cref="InvalidDataException">When the ZIP entry is compressed. Only uncompressed members are supported</exception>
    protected virtual Stream OpenStreamShared(string name)
    {
        long offset = 0;
        long length;
        var stream = OpenArchiveStream();
        var archive = new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen: true);
        var entry = archive.GetEntry(name);
        if (entry == null)
            throw new FileNotFoundException(name);

        // Hacky means of checking that the file isn't compressed
        if (entry.Length != entry.CompressedLength)
            throw new InvalidDataException("File in MZPeak ZIP Archive cannot be stored with compression");

        length = entry.Length;

        // Hacky means of getting the offset of the file contents
        using (var substreamNotSeekable = entry.Open())
            offset = stream.Position;

        // Don't close the shared main stream
        var segStream = new StreamSegment(stream, offset, length, true);
        return segStream;
    }

    void IDisposable.Dispose()
    {

    }
}


public class LocalZipArchive : BaseZipArchive
{
    public string Path;

    public LocalZipArchive(string path, DecryptionConfigurations? decryptionConfigurations = null) : base(decryptionConfigurations)
    {
        Path = path;
        ExtractInitialMetadata();
    }

    public override Stream OpenArchiveStream()
    {
        var stream = File.OpenRead(Path);
        return new BufferedStream(stream);
    }

    public override Stream OpenStream(string name) => OpenStreamIsolated(name);
}


public class ZipArchiveStream<T> : BaseZipArchive, IDisposable where T : Stream
{
    T Stream;

    public ZipArchiveStream(T stream, DecryptionConfigurations? decryptionConfigurations = null) : base(decryptionConfigurations)
    {
        Stream = stream;
        if (!Stream.CanRead) throw new InvalidOperationException("Stream must be readable");
        if (!Stream.CanSeek) throw new InvalidOperationException("Stream must be seekable");
        ExtractInitialMetadata();
    }

    void IDisposable.Dispose() => Stream.Dispose();

    public override ZipArchive OpenArchive()
    {
        var stream = OpenArchiveStream();
        return new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen: true);
    }

    public override Stream OpenArchiveStream()
    {
        return Stream;
    }

    public override Stream OpenStream(string name) => OpenStreamShared(name);
}


public class MemoryMappedZipArchive : BaseZipArchive, IDisposable
{
    public string? Path {get; protected set;}
    public MemoryMappedFile Handle {get; protected set;}

    public MemoryMappedZipArchive(MemoryMappedFile handle)
    {
        Path = null;
        Handle = handle;
        ExtractInitialMetadata();
    }

    public MemoryMappedZipArchive(string path)
    {
        Path = path;

        Handle = MemoryMappedFile.CreateFromFile(
            new FileStream(Path, FileMode.Open, FileAccess.Read, FileShare.Read),
            null,
            0,
            MemoryMappedFileAccess.Read,
            HandleInheritability.Inheritable,
            false
        );
        ExtractInitialMetadata();
    }

    public override Stream OpenArchiveStream() => Handle.CreateViewStream();

    public override Stream OpenStream(string name) => OpenStreamShared(name);

    void IDisposable.Dispose()
    {
        Handle.Dispose();
    }
}


#endregion

#region HTTP Reading

/// <summary>
/// A seekable read-only file stream-like API around an HTTP(S) URL.
///
/// The stream itself is unbuffered. If buffering is desired, wrap in a BufferedStream.
///
/// Requires that the host supports Range Requests (https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Range_requests)
/// </summary>
public class HttpStream : Stream
{
    private static readonly HttpClient defaultHttpClient;

    static HttpStream()
    {
        defaultHttpClient = new HttpClient();
    }

    protected HttpClient? localClient = null;
    public Uri Url;
    protected long _position;
    protected long _length;

    public HttpClient Client
    {
        get => localClient == null ? defaultHttpClient : localClient;
        set => localClient = value;
    }

    public override bool CanRead => true;

    public override bool CanSeek => true;

    public override bool CanWrite => false;

    public override long Length => _length;

    public override long Position { get => _position; set => Seek(value, SeekOrigin.Begin); }

    public HttpStream(Uri uri, HttpClient? client = null)
    {
        Url = uri;
        if (client != null) Client = client;
        _position = 0;
        _length = 0;
        FetchSize();
    }

    public HttpStream(string url, HttpClient? client = null) : this(new Uri(url), client)
    { }

    protected void FetchSize()
    {
        var msg = new HttpRequestMessage()
        {
            Method = HttpMethod.Head,
            RequestUri = Url
        };
        var resp = Client.Send(msg).EnsureSuccessStatusCode();

        var sizeHeader = resp.Content.Headers.GetValues("Content-Length").First();
        _length = Convert.ToInt64(sizeHeader);
    }

    public override void Flush()
    {
        throw new NotImplementedException();
    }

    protected byte[] FetchRange(long start, long end)
    {
        var msg = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = Url,
        };
        msg.Headers.Range = new RangeHeaderValue(start, end);
        var resp = Client.Send(msg).EnsureSuccessStatusCode();
        var stream = resp.Content.ReadAsStream();
        var buf = new byte[end - start];
        stream.Read(buf);
        return buf;
    }

    protected async Task<byte[]> FetchRangeAsync(long start, long end, CancellationToken cancellationToken)
    {
        var msg = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = Url,
        };
        msg.Headers.Range = new RangeHeaderValue(start, end);
        var resp = (await Client.SendAsync(msg, cancellationToken)).EnsureSuccessStatusCode();
        return await resp.Content.ReadAsByteArrayAsync(cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        long bytesToRead = count - offset;
        if (Position + bytesToRead > _length)
        {
            bytesToRead = _length - Position;
        }

        var result = FetchRange(Position, Position + bytesToRead);
        var view = new Span<byte>(buffer);
        result.CopyTo(view.Slice(offset));
        _position += bytesToRead;
        return (int)bytesToRead;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        long bytesToRead = count - offset;
        if (Position + bytesToRead > _length)
        {
            bytesToRead = _length - Position;
        }

        var result = await FetchRangeAsync(Position, Position + bytesToRead, cancellationToken);

        result.CopyTo(buffer, offset);
        _position += result.Length;
        return result.Length;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        var before = _position;
        switch (origin)
        {
            case SeekOrigin.Begin:
                {
                    _position = offset;
                    break;
                }
            case SeekOrigin.Current:
                {
                    _position += offset;
                    break;
                }
            case SeekOrigin.End:
                {
                    _position = _length + offset;
                    break;
                }
        }
        if (_position < 0 || _position > _length)
        {
            var after = _position;
            _position = before;
            throw new InvalidOperationException($"Cannot seek to before the beginning or past the end of the stream! Went from {before} to {after}");
        }
        return _position;
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }
}


public class HttpZipArchive : BaseZipArchive
{
    public Uri Url;
    HttpClient? localClient = null;

    public HttpZipArchive(string url, HttpClient? httpClient = null, DecryptionConfigurations? decryptionConfigs = null) : this(new Uri(url), httpClient, decryptionConfigs) { }

    public HttpZipArchive(Uri url, HttpClient? httpClient = null, DecryptionConfigurations? decryptionConfigs = null) : base(decryptionConfigs)
    {
        Url = url;
        localClient = httpClient;
        ExtractInitialMetadata();
    }

    public override Stream OpenArchiveStream() => new BufferedStream(new HttpStream(Url, localClient));

    public override Stream OpenStream(string name) => OpenStreamIsolated(name);
}

#endregion


public class DirectoryArchive : IMZPeakArchiveStorage
{
    public string Path;
    List<string> fileNames;
    FileIndex fileIndex;
    public DecryptionConfigurations DecryptionConfigurations { get; set; }

    public DirectoryArchive(string path, DecryptionConfigurations? decryptionConfigurations = null)
    {
        Path = path;
        fileNames = new List<string>();
        fileIndex = new FileIndex();
        DecryptionConfigurations = decryptionConfigurations ?? new();
        ExtractInitialMetadata();
    }

    public FileIndex FileIndex() => fileIndex;

    public List<string> FileNames() => fileNames;

    public Stream OpenStream(string name)
    {
        var pathOf = System.IO.Path.Join(Path, name);
        if (!File.Exists(pathOf))
            throw new FileNotFoundException(name);
        return new FileStream(pathOf, FileMode.Open);
    }

    void ExtractInitialMetadata()
    {
        List<string> fileNames = [];
        FileIndex? fileIndex = null;

        foreach (var entry in Directory.EnumerateFileSystemEntries(Path))
        {
            if (!File.Exists(entry)) continue;
            fileNames.Add(entry);
            var fName = System.IO.Path.GetFileName(entry);
            if (fName == Storage.FileIndex.FILE_NAME)
            {
                using (var stream = new StreamReader(File.Open(entry, FileMode.Open)))
                {
                    var indexJson = stream.ReadToEnd();
                    fileIndex = JsonSerializer.Deserialize<FileIndex>(indexJson);

                    if (fileIndex == null)
                    {
                        throw new InvalidDataException("Index JSON file did not deserialize successfully");
                    }
                }
            }
        }
        this.fileNames = fileNames;
        if (fileIndex == null)
            throw new FileNotFoundException("Index JSON file not found");
        this.fileIndex = fileIndex;
    }

    public virtual void Dispose()
    {}
}


public interface IMZPeakArchiveWriter : IDisposable
{
    internal static ILogger? Logger = null;

    public Stream OpenStream(FileIndexEntry indexEntry);

    public FileIndex FileIndex();
}


public class DirectoryArchiveWriter : IMZPeakArchiveWriter
{
    public string Path;
    public FileIndex FileIndex;

    public DirectoryArchiveWriter(string path)
    {
        Path = path;
        FileIndex = new();
    }

    void IDisposable.Dispose()
    {
        var path = System.IO.Path.Join(Path, FileIndex.FILE_NAME);
        using (var stream = File.Create(path))
        {
            var payload = JsonSerializer.Serialize(FileIndex, options: new JsonSerializerOptions() { WriteIndented = true });
            var bytesOf = new UTF8Encoding().GetBytes(payload);
            stream.Write(bytesOf);
        }
    }

    public Stream OpenStream(FileIndexEntry indexEntry)
    {
        var path = System.IO.Path.Join(Path, indexEntry.Name);
        FileIndex.Files.Add(indexEntry);
        return File.Create(path);
    }

    FileIndex IMZPeakArchiveWriter.FileIndex()
    {
        return FileIndex;
    }
}


public class ZipStreamArchiveWriter<T> : IMZPeakArchiveWriter where T : Stream
{
    ZipArchive Archive;
    T OuterStream;
    Stream? CurrentStream;
    ZipArchiveEntry? CurrentEntry;
    long LastStart;
    public FileIndex FileIndex;

    public ZipStreamArchiveWriter(T stream)
    {
        OuterStream = stream;
        Archive = new(OuterStream, ZipArchiveMode.Create, true, System.Text.Encoding.UTF8);
        CurrentStream = null;
        CurrentEntry = null;
        LastStart = 0;
        FileIndex = new();
    }

    void CloseCurrent()
    {
        if (CurrentStream != null)
        {
            IMZPeakArchiveWriter.Logger?.LogDebug($"Closing current stream for {CurrentEntry}");
            CurrentStream.Close();
            IMZPeakArchiveWriter.Logger?.LogDebug($"{(OuterStream.Position - LastStart) / 1000000.0} MB written");
            CurrentStream = null;
            CurrentEntry = null;
        }
    }

    void IDisposable.Dispose()
    {
        CloseCurrent();
        var entry = Archive.CreateEntry(FileIndex.FILE_NAME, CompressionLevel.NoCompression);
        using (var stream = entry.Open())
        {
            IMZPeakArchiveWriter.Logger?.LogDebug("Writing file index");
            var payload = JsonSerializer.Serialize(FileIndex, options: new JsonSerializerOptions() { WriteIndented = true });
            var bytesOf = new UTF8Encoding().GetBytes(payload);
            stream.Write(bytesOf);
        }
        IMZPeakArchiveWriter.Logger?.LogDebug("Closing ZIP archive");
        Archive.Dispose();
    }

    public Stream OpenStream(FileIndexEntry indexEntry)
    {
        CloseCurrent();
        IMZPeakArchiveWriter.Logger?.LogDebug($"Opening {indexEntry}");
        var entry = Archive.CreateEntry(indexEntry.Name, CompressionLevel.NoCompression);
        LastStart = OuterStream.Position;
        CurrentStream = entry.Open();
        CurrentEntry = entry;
        FileIndex.Files.Add(indexEntry);
        return CurrentStream;
    }

    FileIndex IMZPeakArchiveWriter.FileIndex() => FileIndex;
}
