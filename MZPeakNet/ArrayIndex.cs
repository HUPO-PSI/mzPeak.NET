namespace MZPeak.Metadata;

using System.Text.RegularExpressions;
using System.Text.Json.Serialization;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;
using System.Net.NetworkInformation;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BufferFormat
{
    [JsonStringEnumMemberName("point")]
    Point,
    [JsonStringEnumMemberName("chunk_values")]
    ChunkValues,
    [JsonStringEnumMemberName("chunk_start")]
    ChunkStart,
    [JsonStringEnumMemberName("chunk_end")]
    ChunkEnd,
    [JsonStringEnumMemberName("chunk_encoding")]
    ChunkEncoding,
    [JsonStringEnumMemberName("chunk_secondary")]
    ChunkSecondary,
    [JsonStringEnumMemberName("chunk_transform")]
    ChunkTransform,
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BufferPriority
{
    [JsonStringEnumMemberName("primary")]
    Primary,
    [JsonStringEnumMemberName("secondary")]
    Secondary
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BufferContext
{
    [JsonStringEnumMemberName("spectrum")]
    Spectrum,
    [JsonStringEnumMemberName("chromatogram")]
    Chromatogram,
}

static class BufferContexteMethods
{
    public static string IndexName(this BufferContext bufferContext)
    {
        switch (bufferContext)
        {
            case BufferContext.Spectrum:
                {
                    return "spectrum_index";
                }
            case BufferContext.Chromatogram:
                {
                    return "chromatogram_index";
                }
            default:
                {
                    throw new InvalidOperationException("Cannot create index column name for `Other`");
                }
        }
    }

    public static string Name(this BufferContext bufferContext)
    {
        switch (bufferContext)
        {
            case BufferContext.Spectrum:
                {
                    return "spectrum";
                }
            case BufferContext.Chromatogram:
                {
                    return "chromatogram";
                }
            default:
                {
                    throw new InvalidOperationException("Cannot create index column name for `Other`");
                }
        }
    }

    public static ArrayType DefaultPrimaryAxis(this BufferContext bufferContext)
    {
        switch(bufferContext)
        {
            case BufferContext.Spectrum:
                {
                    return ArrayType.MZArray;
                }
            case BufferContext.Chromatogram:
                {
                    return ArrayType.TimeArray;
                }
            default: throw new InvalidOperationException("Unknown axis type for `Other`");
        }
    }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public record ArrayIndexEntry
{
    [JsonPropertyName("context")]
    public required BufferContext Context { get; set; }

    [JsonPropertyName("path")]
    public required string Path { get; set; }

    [JsonPropertyName("data_type")]
    public required string DataTypeCURIE { get; set; }

    [JsonPropertyName("array_type")]
    public required string ArrayTypeCURIE { get; set; }

    [JsonPropertyName("array_name")]
    public required string ArrayName { get; set; }

    [JsonPropertyName("unit")]
    public string? UnitCURIE { get; set; } = null;

    [JsonPropertyName("transform")]
    public string? Transform { get; set; } = null;

    [JsonPropertyName("buffer_format")]
    public BufferFormat BufferFormat { get; set; }

    [JsonPropertyName("data_processing_id")]
    public string? DataProcessesingId { get; set; } = null;

    [JsonPropertyName("buffer_priority")]
    public BufferPriority? BufferPriority { get; set; } = null;

    [JsonPropertyName("sorting_rank")]
    public uint? SortingRank { get; set; } = null;

    [JsonIgnore]
    public int? SchemaIndex { get; set; } = null;

    public ArrowType GetArrowType()
    {
        switch (DataTypeCURIE)
        {
            case "MS:1000521":
                {
                    return new FloatType();
                }
            case "MS:1000519":
                {
                    return new Int32Type();
                }
            case "MS:1000522":
                {
                    return new Int64Type();
                }
            case "MS:1000523":
                {
                    return new DoubleType();
                }
            case "MS:1001479":
                {
                    return new LargeStringType();
                }
            default:
                {
                    throw new InvalidDataException("Cannot map " + DataTypeCURIE + " to an Arrow type");
                }
        }
    }

    public string CreateColumnName()
    {
        var notAlpha = new Regex("[^A-Za-z_]+");
        var arrayName = notAlpha.Replace(ArrayName.Replace("m/z", "mz").Replace(" array", ""), "_");
        if (BufferPriority == Metadata.BufferPriority.Primary)
        {
            return arrayName;
        } else
        {
            var dtypeName = BinaryDataTypeMethods.FromCURIE[DataTypeCURIE].NameForColumn();
            var unitName = UnitCURIE != null ? UnitMethods.FromCURIE[UnitCURIE].NameForColumn() : null;
            if (unitName != null)
            {
                return string.Join("_", [arrayName, dtypeName, unitName]);
            }
            else
            {
                return string.Join("_", [arrayName, dtypeName]);
            }
        }
    }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public class ArrayIndex
{
    [JsonPropertyName("prefix")]
    public string Prefix { get; set; }

    [JsonPropertyName("entries")]
    public List<ArrayIndexEntry> Entries { get; set; }

    public int Length { get => Entries.Count; }

    public ArrayIndex()
    {
        Prefix = "?";
        Entries = new();
    }

    public ArrayIndex(string prefix, List<ArrayIndexEntry> entries)
    {
        Prefix = prefix;
        Entries = entries;
    }
}


public class ArrayIndexBuilder
{
    string Prefix;
    List<ArrayIndexEntry> Entries;
    BufferContext Context;
    BufferFormat Format;

    internal ArrayIndexBuilder(string prefix, BufferContext context, BufferFormat bufferFormat)
    {
        Prefix = prefix;
        Context = context;
        Entries = new();
        Format = bufferFormat;
    }

    public static ArrayIndexBuilder PointBuilder(BufferContext context)
    {
        return new("point", context, BufferFormat.Point);
    }

    public static ArrayIndexBuilder ChunkBuilder(BufferContext context)
    {
        return new("chunk", context, BufferFormat.ChunkValues);
    }

    public ArrayIndexBuilder Add(ArrayType arrayType, BinaryDataType dataType, Unit? unit=null, uint? sortingRank=null, string? transform=null)
    {
        var entry = new ArrayIndexEntry()
        {
            ArrayName = arrayType.Name(),
            ArrayTypeCURIE = arrayType.CURIE(),
            Context = Context,
            DataTypeCURIE = dataType.CURIE(),
            UnitCURIE = unit?.CURIE(),
            SchemaIndex = null,
            Path = Prefix,
            SortingRank = sortingRank,
            Transform = transform,
        };
        entry.Path = $"{Prefix}.{entry.CreateColumnName()}";
        return this;
    }
}