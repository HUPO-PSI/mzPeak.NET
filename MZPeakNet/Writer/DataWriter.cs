using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;

namespace MZPeak.Writer.Data;


public class BaseLayoutBuilder
{
    public BufferContext BufferContext { get; protected set; }

    public bool ShouldRemoveZeroRuns { get; set; } = true;

    protected UInt64Array.Builder Index;
    protected List<IArrowArrayBuilder> Arrays;
    protected List<IArrowType> DataTypes;
    public ArrayIndex ArrayIndex { get; protected set; }

    public BaseLayoutBuilder(ArrayIndex arrayIndex)
    {
        ArrayIndex = arrayIndex;
        Index = new();
        Arrays = new();
        DataTypes = new();
        int i = 1;
        foreach (var entry in ArrayIndex.Entries)
        {
            entry.SchemaIndex = i;
            i++;
            BufferContext = entry.Context;
            switch (entry.DataTypeCURIE)
            {
                case "MS:1000523":
                    {
                        DataTypes.Add(new DoubleType());
                        Arrays.Add(new DoubleArray.Builder());
                        break;
                    }
                case "MS:1000521":
                    {
                        DataTypes.Add(new FloatType());
                        Arrays.Add(new FloatArray.Builder());
                        break;
                    }
                case "MS:1000519":
                    {
                        DataTypes.Add(new Int32Type());
                        Arrays.Add(new Int32Array.Builder());
                        break;
                    }
                case "MS:1000522":
                    {
                        DataTypes.Add(new Int64Type());
                        Arrays.Add(new Int64Array.Builder());
                        break;
                    }
                default:
                    {
                        throw new NotImplementedException(entry.DataTypeCURIE);
                    }
            }
        }
    }
}


public class PointLayoutBuilder : BaseLayoutBuilder
{
    public ulong NumberOfPoints = 0;

    public PointLayoutBuilder(ArrayIndex arrayIndex) : base(arrayIndex)
    {}

    public void Preprocess(ulong entryIndex, Dictionary<ArrayIndexEntry, IArrowArray> arrays)
    {
        List<(ArrayIndexEntry, IArrowArray)> notCoveredArrays = new();
        ArrayIndexEntry? nullInterpolate = null;
        ArrayIndexEntry? nullZero = null;
        ArrayIndexEntry? intensityArray = null;
        foreach (var col in arrays)
        {
            if (!ArrayIndex.Entries.Contains(col.Key))
            {
                notCoveredArrays.Add((col.Key, col.Value));
                continue;
            }
            if (col.Key.ArrayTypeCURIE == ArrayType.IntensityArray.CURIE()) intensityArray = col.Key;
            if (col.Key.Transform == NullInterpolation.NullInterpolateCURIE) nullInterpolate = col.Key;
            else if (col.Key.Transform == NullInterpolation.NullZeroCURIE) nullZero = col.Key;
        }
        foreach(var (k, v) in notCoveredArrays) arrays.Remove(k);

        if (intensityArray != null)
        {
            var intensityArrayVal = arrays[intensityArray];
            switch (intensityArrayVal.Data.DataType.TypeId)
            {
                case ArrowTypeId.Float:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((FloatArray)intensityArrayVal);
                        break;
                    }
                case ArrowTypeId.Double:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((DoubleArray)intensityArrayVal);
                        break;
                    }
                case ArrowTypeId.Int32:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((Int32Array)intensityArrayVal);
                        break;
                    }
                case ArrowTypeId.Int64:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((Int64Array)intensityArrayVal);
                        break;
                    }
                default:
                    throw new NotImplementedException();
            }
        }

        if (nullInterpolate != null && nullZero != null)
        {

        } else if (nullInterpolate != null || nullZero != null) throw new InvalidOperationException();
    }

    public int AddPoints(ulong entryIndex, Dictionary<ArrayIndexEntry, IArrowArray> arrays)
    {
        int k = 0;
        foreach(var val in arrays.Values)
        {
            if(k > 0 && k != val.Length) throw new InvalidDataException("Arrays do not have equal lengths");
            else k = val.Length;
        }
        Index.AppendRange(Enumerable.Repeat(entryIndex, k));
        foreach(var entry in ArrayIndex.Entries)
        {
            if (entry.SchemaIndex == null) throw new InvalidOperationException("Cannot be null");
            IArrowArray? array;
            if (arrays.TryGetValue(entry, out array))
            {
                var builder = Arrays[(int)entry.SchemaIndex];
                var dtype = DataTypes[(int)entry.SchemaIndex];
                switch (dtype.TypeId)
                {
                    case ArrowTypeId.Double:
                        {
                            DoubleArray valArray = (DoubleArray)array;
                            DoubleArray.Builder valBuilder = (DoubleArray.Builder)builder;
                            foreach(var v in valArray) valBuilder.Append(v);
                            break;
                        }
                    case ArrowTypeId.Float:
                        {
                            FloatArray valArray = (FloatArray)array;
                            FloatArray.Builder valBuilder = (FloatArray.Builder)builder;
                            foreach (var v in valArray) valBuilder.Append(v);
                            break;
                        }
                    case ArrowTypeId.Int32:
                        {
                            Int32Array valArray = (Int32Array)array;
                            Int32Array.Builder valBuilder = (Int32Array.Builder)builder;
                            foreach (var v in valArray) valBuilder.Append(v);
                            break;
                        }
                    case ArrowTypeId.Int64:
                        {
                            Int64Array valArray = (Int64Array)array;
                            Int64Array.Builder valBuilder = (Int64Array.Builder)builder;
                            foreach (var v in valArray) valBuilder.Append(v);
                            break;
                        }
                    default: throw new NotImplementedException();
                }
            } else
            {
                var builder = Arrays[(int)entry.SchemaIndex];
                var dtype = DataTypes[(int)entry.SchemaIndex];
                switch (dtype.TypeId)
                {
                    case ArrowTypeId.Double:
                        {
                            for(var j = 0; j < k; j++)
                                ((DoubleArray.Builder)builder).AppendNull();
                            break;
                        }
                    case ArrowTypeId.Float:
                        {
                            for (var j = 0; j < k; j++)
                                ((FloatArray.Builder)builder).AppendNull();
                            break;
                        }
                    case ArrowTypeId.Int32:
                        {
                            for (var j = 0; j < k; j++)
                                ((Int32Array.Builder)builder).AppendNull();
                            break;
                        }
                    case ArrowTypeId.Int64:
                        {
                            for (var j = 0; j < k; j++)
                                ((Int64Array.Builder)builder).AppendNull();
                            break;
                        }
                    default: throw new NotImplementedException();
                }
            }
        }
        NumberOfPoints += (ulong)k;

        return 0;
    }
}