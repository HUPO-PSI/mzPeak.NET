namespace MZPeak.Compute;

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Reader;


public class NullInterpolation
{
    public const string NullInterpolateCURIE = "MS:1003901";
    public const string NullZeroCURIE = "MS:1003902";

    public static List<T> CollectDeltas<T>(IEnumerable<T?> values) where T : struct, INumber<T>
    {
        List<T> deltas = new();
        T last = default;
        int i = 0;
        foreach (var value in values)
        {
            if (value == null)
            {
                continue;
            }
            if (i == 0)
            {
                last = (T)value;
                i++;
            }
            else
            {
                var delta = (T)value - last;
                deltas.Add(delta);
                last = (T)value;
            }
        }
        deltas.Sort();
        return deltas;
    }

    public static T SortedMedian<T>(List<T> values) where T : struct, INumber<T>
    {
        if (values.Count == 0)
        {
            return T.Zero;
        }
        else if (values.Count <= 2)
        {
            return values[0];
        }
        else
        {
            int mid = values.Count / 2;
            if (values.Count % 2 == 0)
            {
                return values[mid];
            }
            else
            {
                return (values[mid] + values[mid + 1]) / (T.One + T.One);
            }
        }
    }

    public static List<(int, int)> FindNullBounds(IArrowArray arrayValues)
    {
        List<(int, int)> bounds = new();
        if (arrayValues.Length == 0) return bounds;
        List<int> nullHere = new();
        for (int i = 0; i < arrayValues.Length; i++)
        {
            if (arrayValues.IsNull(i))
            {
                nullHere.Add(i);
            }
        }
        if (nullHere.Count == 0)
        {
            bounds.Add((0, arrayValues.Length));
            return bounds;
        }
        if (nullHere[0] != 0)
        {
            List<int> tmp = [0, .. nullHere];
            nullHere = tmp;
        }
        if (nullHere.Last() != arrayValues.Length - 1)
        {
            nullHere.Add(arrayValues.Length);
        }
        if (nullHere.Count % 2 != 0)
        {
            throw new InvalidDataException("The nulls in this data array are not properly paired");
        }
        for (int i = 0; i < nullHere.Count; i += 2)
        {
            bounds.Add((nullHere[i], nullHere[i + 1]));
        }
        return bounds;
    }

    public static void FillNullsWithModel<T, TBuilder>(PrimitiveArray<T> arrayValues, SpacingInterpolationModel<T> model, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> builder) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        var bounds = FindNullBounds(arrayValues);
        foreach (var (startIdx, endIdx) in bounds)
        {
            var chunk = (PrimitiveArray<T>)arrayValues.Slice(startIdx, endIdx - startIdx);
            var n = chunk.Length;
            // NullCount can only be 0, 1, or 2
            var nHasReal = n - chunk.NullCount;

            if (nHasReal == 1)
            {
                if (n == 2)
                {
                    if (chunk.IsNull(0))
                    {
                        var vAt = chunk.GetValue(1);
                        if (vAt == null) throw new InvalidDataException("Cannot both be null");
                        var vFill = (T)vAt - model.Predict((T)vAt);
                        builder.Append(vFill);
                        builder.Append((T)vAt);
                    }
                    else
                    {
                        var vAt = chunk.GetValue(0);
                        if (vAt == null) throw new InvalidDataException("Cannot both be null");
                        var vFill = (T)vAt + model.Predict((T)vAt);
                        builder.Append((T)vAt);
                        builder.Append(vFill);
                    }
                }
                else if (n == 3)
                {
                    var vAt = chunk.GetValue(1);
                    if (vAt == null) throw new InvalidDataException("Cannot both be null");
                    var vFill = (T)vAt - model.Predict((T)vAt);
                    builder.Append(vFill);
                    builder.Append((T)vAt);
                    vFill = (T)vAt + model.Predict((T)vAt);
                    builder.Append(vFill);
                }
                else throw new InvalidOperationException("This is impossible");
            }
            else
            {
                var delta = LocalMedianDelta(chunk);
                if (chunk.IsNull(0))
                {
                    var vAt = chunk.GetValue(1);
                    if (vAt == null) throw new InvalidDataException("Cannot both be null");
                    var vFill = (T)vAt - delta;
                    builder.Append(vFill);
                }

                foreach (var v in (PrimitiveArray<T>)chunk.Slice(1, chunk.Length - 2))
                {
                    if (v == null) throw new InvalidDataException("Cannot both be null");
                    builder.Append((T)v);
                }
                if (chunk.IsNull(chunk.Length - 1))
                {
                    var vAt = chunk.GetValue(chunk.Length - 2);
                    if (vAt == null) throw new InvalidDataException("Cannot both be null");
                    builder.Append((T)vAt + delta);
                }
            }
        }
    }

    public static T LocalMedianDelta<T>(PrimitiveArray<T> arrayValues) where T : struct, INumber<T>
    {
        var deltas = CollectDeltas(arrayValues);
        if (deltas.Count == 0)
        {
            return T.Zero;
        }
        var median = SortedMedian(deltas);
        var deltasBelow = deltas.Where(v => v <= median).ToList();
        if (deltasBelow.Count == 0)
        {
            return median;
        }
        else
        {
            return SortedMedian(deltasBelow);
        }
    }
}

public class NoCompressionCodec
{
    public const string CURIE = "MS:1000576";

    public static int Encode<T, TBuilder>(T startValue, IEnumerable<T?> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;
        foreach (var value in values)
        {
            if (value == null)
            {
                nNulls += 1;
                accumulator.AppendNull();
            }
            else
            {
                accumulator.Append((T)value);
            }
        }
        return nNulls;
    }

    public static int Decode<T, TBuilder>(T startValue, PrimitiveArray<T> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;
        accumulator.Append(startValue);
        foreach (var value in values)
        {
            if (value == null)
            {
                nNulls += 1;
                accumulator.AppendNull();
            }
            else
            {
                accumulator.Append((T)value);
            }
        }
        return nNulls;
    }
}

public class DeltaCodec
{
    public const string CURIE = "MS:1003089";

    public static int Encode<T, TBuilder>(T? startValue, IEnumerable<T?> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;

        T? last = startValue;

        if (last == null)
        {
            nNulls += 1;
            accumulator.AppendNull();
        }
        foreach (var value in values)
        {
            if (value != null)
            {
                if (last != null)
                {
                    accumulator.Append((T)value - (T)last);
                }
                else
                {
                    accumulator.Append((T)value);
                }
                last = value;
            }
            else
            {
                accumulator.AppendNull();
                last = value;
                nNulls += 1;
            }
        }

        return nNulls;
    }

    public static int Decode<T, TBuilder>(T startValue, PrimitiveArray<T> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;
        if (values.Length < 2)
        {
            throw new IndexOutOfRangeException("Cannot have a delta encoded chunk value slice size of less than two");
        }
        T? last = startValue;
        if (values.ElementAt(0) == null)
        {
            if (values.ElementAt(1) == null)
            {
                accumulator.Append(startValue);
            }
            last = default;
        }
        else
        {
            accumulator.Append(startValue);
        }

        foreach (var value in values)
        {
            if (value != null)
            {
                if (last == null)
                {
                    last = value;
                    accumulator.Append((T)value);
                }
                else
                {
                    last = last + value;
                    accumulator.Append((T)last);
                }
            }
            else
            {
                nNulls += 1;
                last = value;
                accumulator.AppendNull();
            }
        }
        return nNulls;
    }
}


public static class Compute
{
    public static void NullToZero<T, TBuilder>(PrimitiveArray<T> array, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T: struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        foreach(var value in array)
        {
            accumulator.Append(value == null ? T.Zero : (T)value);
        }
    }

    public static IArrowArray NullToZero<T>(PrimitiveArray<T> array) where T: struct, INumber<T> {
        switch(array.Data.DataType.TypeId) {
            case ArrowTypeId.Double:
                {
                    var builder = new DoubleArray.Builder();
                    NullToZero((DoubleArray)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Float:
                {
                    var builder = new FloatArray.Builder();
                    NullToZero((FloatArray)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int32:
                {
                    var builder = new Int32Array.Builder();
                    NullToZero((Int32Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int64:
                {
                    var builder = new Int64Array.Builder();
                    NullToZero((Int64Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt32:
                {
                    var builder = new UInt32Array.Builder();
                    NullToZero((UInt32Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt64:
                {
                    var builder = new UInt64Array.Builder();
                    NullToZero((UInt64Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int16:
                {
                    var builder = new Int16Array.Builder();
                    NullToZero((Int16Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int8:
                {
                    var builder = new Int8Array.Builder();
                    NullToZero((Int8Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt16:
                {
                    var builder = new UInt16Array.Builder();
                    NullToZero((UInt16Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt8:
                {
                    var builder = new UInt8Array.Builder();
                    NullToZero((UInt8Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }
}