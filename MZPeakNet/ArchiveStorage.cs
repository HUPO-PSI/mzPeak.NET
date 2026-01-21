namespace MZPeak.Storage;

using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.IO.Compression;

using ParquetSharp.IO;
using MZPeak.Metadata;
using System.Text;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum EntityType
{
    [JsonStringEnumMemberName("spectrum")]
    Spectrum,
    [JsonStringEnumMemberName("chromatogram")]
    Chromatogram,
    [JsonStringEnumMemberName("other")]
    Other
}


[JsonConverter(typeof(JsonStringEnumConverter))]
public enum DataKind
{
    [JsonStringEnumMemberName("data arrays")]
    DataArrays,
    [JsonStringEnumMemberName("metadata")]
    Metadata,
    [JsonStringEnumMemberName("peaks")]
    Peaks,
    [JsonStringEnumMemberName("other")]
    Other,
    [JsonStringEnumMemberName("proprietary")]
    Proprietary
}


[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public class FileIndexEntry
{
    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("entity_type")]
    public EntityType EntityType { get; set; }

    [JsonPropertyName("data_kind")]
    public DataKind DataKind { get; set; }

    public static FileIndexEntry FromEntityAndData(EntityType entityType, DataKind dataKind)
    {
        string entityTypeTag = "";
        switch (entityType)
        {
            case EntityType.Chromatogram:
                {
                    entityTypeTag = "chromatograms";
                    break;
                }
            case EntityType.Spectrum:
                {
                    entityTypeTag = "spectra";
                    break;
                }
            case EntityType.Other:
                {
                    throw new NotImplementedException(entityType.ToString());
                }
        }
        string dataKindTag = "";
        switch (dataKind)
        {
            case DataKind.DataArrays:
                {
                    dataKindTag = "data";
                    break;
                }
            case DataKind.Metadata:
                {
                    dataKindTag = "metadata";
                    break;
                }
            case DataKind.Peaks:
                {
                    dataKindTag = "peaks";
                    break;
                }
            case DataKind.Proprietary:
                {
                    throw new NotImplementedException(dataKind.ToString());
                }
            case DataKind.Other:
                {
                    throw new NotImplementedException(dataKind.ToString());
                }
        }
        return new FileIndexEntry(
            string.Format("{0}_{1}.parquet", entityTypeTag, dataKindTag),
            entityType,
            dataKind
        );
    }

    public FileIndexEntry(string name, EntityType entityType, DataKind dataKind)
    {
        Name = name;
        EntityType = entityType;
        DataKind = dataKind;
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
        return Files.Find((entry) => entry.DataKind == dataKind && entry.EntityType == entityType);
    }

    public FileIndex()
    {
        Files = new List<FileIndexEntry>();
        Metadata = new JsonObject();
    }
}


public interface IMZPeakArchiveStorage
{
    public List<string> FileNames();

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

    public ParquetSharp.Arrow.FileReader? SpectrumData()
    {
        var stream = OpenEntry(EntityType.Spectrum, DataKind.DataArrays);
        return stream == null ? null : new ParquetSharp.Arrow.FileReader(new ManagedRandomAccessFile(stream));
    }

    public ParquetSharp.Arrow.FileReader? SpectrumPeaks()
    {
        var stream = OpenEntry(EntityType.Spectrum, DataKind.Peaks);
        return stream == null ? null : new ParquetSharp.Arrow.FileReader(new ManagedRandomAccessFile(stream));
    }

    public ParquetSharp.Arrow.FileReader? ChromatogramData()
    {
        var stream = OpenEntry(EntityType.Chromatogram, DataKind.DataArrays);
        return stream == null ? null : new ParquetSharp.Arrow.FileReader(new ManagedRandomAccessFile(stream));
    }

    public ParquetSharp.Arrow.FileReader? SpectrumMetadata()
    {
        var stream = OpenEntry(EntityType.Spectrum, DataKind.Metadata);
        return stream == null ? null : new ParquetSharp.Arrow.FileReader(new ManagedRandomAccessFile(stream));
    }

    public ParquetSharp.Arrow.FileReader? ChromatogramMetadata()
    {
        var stream = OpenEntry(EntityType.Chromatogram, DataKind.Metadata);
        return stream == null ? null : new ParquetSharp.Arrow.FileReader(new ManagedRandomAccessFile(stream));
    }

    public Stream OpenStream(string name);

    public FileIndex FileIndex();
}


public class StreamSegment : Stream
{
    Stream Stream;

    long Offset;

    long _length;

    public StreamSegment(Stream stream, long offset, long length)
    {
        Stream = stream;
        Offset = offset;
        _length = length;
    }

    public new void Dispose()
    {
        Stream.Dispose();
    }

    public override bool CanRead => true;

    public override bool CanSeek => true;

    public override bool CanWrite => false;

    public override long Length => _length;

    public override long Position {
        get => Stream.Position - Offset;
        set => Stream.Position = Offset + value;
    }

    public override void Flush()
    {
        Stream.Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        long bytesToRead = count - offset;
        if (Position + bytesToRead > _length)
        {
            bytesToRead = _length - Position;
        }
        return Stream.Read(buffer, offset, (int)bytesToRead);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        switch (origin) {
            case SeekOrigin.Begin:
            {
                Position = offset < _length ? offset : _length;
                break;
            }
            case SeekOrigin.Current:
            {
                Position = Position + offset < _length ? Position + offset : _length;
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

    private void Configure()
    {
        Stream.Seek((long)Offset, SeekOrigin.Begin);
    }
}


public class LocalZipArchive : IMZPeakArchiveStorage
{
    public string Path;
    List<string> fileNames;
    FileIndex fileIndex;


    public LocalZipArchive(string path)
    {
        Path = path;
        fileNames = new List<string>();
        fileIndex = new FileIndex();
        extractInitialMetadata();
    }

    public List<string> FileNames()
    {
        return fileNames;
    }

    public FileIndex FileIndex()
    {
        return fileIndex;
    }

    public Stream OpenStream(string name)
    {
        var stream = File.OpenRead(Path);
        var archive = new ZipArchive(stream, ZipArchiveMode.Read);
        var entry = archive.GetEntry(name);
        if (entry == null)
        {
            throw new FileNotFoundException(name);
        }

        // Hacky means of checking that the file isn't compressed
        if (entry.Length != entry.CompressedLength)
        {
            throw new IOException("File in MZPeak ZIP Archive cannot be stored with compression");
        }

        var length = entry.Length;

        // Hacky means of getting the offset of the file contents
        var substreamNotSeekable = entry.Open();
        var offset = stream.Position;
        substreamNotSeekable.Close();

        stream.Close();
        stream = File.OpenRead(Path);
        var segStream = new StreamSegment(stream, offset, length);
        return segStream;
    }

    void extractInitialMetadata()
    {
        List<string> fileNames = [];
        var archive = ZipFile.OpenRead(Path);
        FileIndex? fileIndex = null;
        foreach (var entry in archive.Entries)
        {
            fileNames.Add(entry.Name);
            if (entry.Name == Storage.FileIndex.FILE_NAME)
            {
                using (var stream = new StreamReader(entry.Open()))
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
        archive.Dispose();
        this.fileNames = fileNames;
        if (fileIndex == null)
        {
            throw new FileNotFoundException("Index JSON file not found");
        }
        this.fileIndex = fileIndex;
    }
}


public class DirectoryArchive : IMZPeakArchiveStorage
{
    public string Path;
    List<string> fileNames;
    FileIndex fileIndex;

    public DirectoryArchive(string path)
    {
        Path = path;
        fileNames = new List<string>();
        fileIndex = new FileIndex();
        extractInitialMetadata();
    }

    public FileIndex FileIndex()
    {
        return fileIndex;
    }

    public List<string> FileNames()
    {
        return fileNames;
    }

    public Stream OpenStream(string name)
    {
        var pathOf = System.IO.Path.Join(Path, name);
        if (!File.Exists(pathOf))
        {
            throw new FileNotFoundException(name);
        }
        return new FileStream(pathOf, FileMode.Open);
    }

    void extractInitialMetadata()
    {
        List<string> fileNames = [];
        FileIndex? fileIndex = null;

        foreach (var entry in Directory.EnumerateFileSystemEntries(Path))
        {
            if(!File.Exists(entry)) continue;

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
        {
            throw new FileNotFoundException("Index JSON file not found");
        }
        this.fileIndex = fileIndex;
    }
}


public interface IMZPeakArchiveWriter : IDisposable
{
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

    public void Dispose()
    {
        var path = System.IO.Path.Join(Path, Storage.FileIndex.FILE_NAME);
        using (var stream = File.OpenWrite(path))
        {
            var payload = JsonSerializer.Serialize(FileIndex);
            var bytesOf = new UTF8Encoding().GetBytes(payload);
            stream.Write(bytesOf);
        }
    }

    public Stream OpenStream(FileIndexEntry indexEntry)
    {
        var path = System.IO.Path.Join(Path, indexEntry.Name);
        FileIndex.Files.Add(indexEntry);
        return File.OpenWrite(path);
    }

    FileIndex IMZPeakArchiveWriter.FileIndex()
    {
        return FileIndex;
    }
}