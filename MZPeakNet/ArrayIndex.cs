namespace MZPeak.Metadata;

using System.Text.RegularExpressions;
using System.Text.Json.Serialization;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;
using System.Net.NetworkInformation;
using System.ComponentModel.DataAnnotations;
using Apache.Arrow;
using System.Net;
using System.IO.Compression;
using MZPeak.Reader.Visitors;

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

    public ArrayType? GetArrayType()
    {
        ArrayType v;
        if (ArrayTypeMethods.FromCURIE.TryGetValue(ArrayTypeCURIE, out v)) return v;
        else return null;
    }

    public Unit? GetUnit()
    {
        if (UnitCURIE == null) return null;
        Unit v;
        if (UnitMethods.FromCURIE.TryGetValue(UnitCURIE, out v)) return v;
        else return null;
    }

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

    public ArrayIndexBuilder Add(ArrayType arrayType, BinaryDataType dataType, Unit? unit=null, uint? sortingRank=null, string? transform=null, BufferPriority? priority=null)
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
            BufferPriority = priority
        };
        entry.Path = $"{Prefix}.{entry.CreateColumnName()}";
        Entries.Add(entry);
        return this;
    }

    public void MarkPriorities()
    {
        Dictionary<ArrayType, int> indexOfFirst = new();
        Dictionary<ArrayType, bool> hadPriority = new();
        for (var i = 0; i < Entries.Count; i++)
        {
            var tp = ArrayTypeMethods.FromCURIE[Entries[i].ArrayTypeCURIE];
            if (Entries[i].BufferPriority == BufferPriority.Primary)
            {
                hadPriority[tp] = true;
            }
            if (!indexOfFirst.ContainsKey(tp) && Entries[i].BufferPriority == null)
            {
                indexOfFirst[tp] = i;
            }
        }
        foreach(var kv in indexOfFirst)
        {
            bool hadPriorityFor;
            if (!hadPriority.TryGetValue(kv.Key, out hadPriorityFor))
            {
                hadPriorityFor = false;
            }
            if (hadPriorityFor) continue;
            Entries[kv.Value].BufferPriority = BufferPriority.Primary;
        }
        foreach(var entry in Entries)
        {
            entry.Path = $"{Prefix}.{entry.CreateColumnName()}";
        }
    }

    public ArrayIndex Build()
    {
        MarkPriorities();
        return new ArrayIndex(Prefix, Entries);
    }
}


public class AuxiliaryArray :  IHasParameters
{
    public Memory<byte> Data;
    public Param Name;
    public BinaryDataType DataType;
    public Compression Compression;
    public Unit? Unit;
    public List<Param> Parameters;
    public ArrowType ArrowType => DataType.ArrowType();

    List<Param> IHasParameters.Parameters { get => Parameters; set => Parameters = value; }

    public AuxiliaryArray(Memory<byte> data, Param name, BinaryDataType dataType, Unit? unit, Compression compression=Compression.NoCompression, List<Param>? parameters=null)
    {
        Data = data;
        Name = name;
        DataType = dataType;
        Unit = unit;
        Compression = compression;
        Parameters = parameters ?? new();
    }

    public ReadOnlySpan<T> View<T>() where T: struct
    {
        if (Compression == Compression.NoCompression) return Data.Span.CastTo<T>();
        switch (Compression)
        {
            case Compression.NoCompression: return Data.Span.CastTo<T>();
            case Compression.Zlib: throw new NotImplementedException();
            case Compression.Zstd: throw new NotImplementedException();
            default: throw new NotImplementedException();
        }
    }

    public static AuxiliaryArray FromValues<T>(List<T> values, ArrayIndexEntry entry) where T : struct
    {
        var bytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(values)).ToArray();
        var name = new Param(entry.ArrayName, entry.ArrayTypeCURIE, null, entry.UnitCURIE);
        var dataType = BinaryDataTypeMethods.FromCURIE[entry.DataTypeCURIE];
        var unit = entry.GetUnit();
        return new AuxiliaryArray(bytes, name, dataType, unit, Compression.NoCompression);
    }

    public static AuxiliaryArray FromValues(IArrowArray values, ArrayIndexEntry entry)
    {
        switch (values.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return FromValues((DoubleArray)values, entry);
            case ArrowTypeId.Float:
                return FromValues((FloatArray)values, entry);
            case ArrowTypeId.Int32:
                return FromValues((Int32Array)values, entry);
            case ArrowTypeId.Int64:
                return FromValues((Int64Array)values, entry);
            case ArrowTypeId.UInt32:
                return FromValues((UInt32Array)values, entry);
            case ArrowTypeId.UInt64:
                return FromValues((UInt64Array)values, entry);
            case ArrowTypeId.Int16:
                return FromValues((Int16Array)values, entry);
            case ArrowTypeId.Int8:
                return FromValues((Int8Array)values, entry);
            case ArrowTypeId.UInt16:
                return FromValues((UInt16Array)values, entry);
            case ArrowTypeId.UInt8:
                return FromValues((UInt8Array)values, entry);
            default:
                throw new InvalidDataException("Unsupported data type " + values.Data.DataType.Name);
        }
    }

    public static AuxiliaryArray FromValues<T>(PrimitiveArray<T> values, ArrayIndexEntry entry) where T : struct, System.Numerics.INumber<T>
    {
        var bytes = values.ValueBuffer.Memory.ToArray();
        var name = new Param(entry.ArrayName, entry.ArrayTypeCURIE, null, entry.UnitCURIE);
        var dataType = BinaryDataTypeMethods.FromCURIE[entry.DataTypeCURIE];
        var unit = entry.GetUnit();
        return new AuxiliaryArray(bytes, name, dataType, unit, Compression.NoCompression);
    }
}