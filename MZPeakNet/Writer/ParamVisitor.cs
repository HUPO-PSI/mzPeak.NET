using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using MZPeak.ControlledVocabulary;

namespace MZPeak.Writer.Visitors;


public class ParamValueBuilder : IArrowBuilder<string>, IArrowBuilder<long>, IArrowBuilder<double>, IArrowBuilder<bool>
{
    public StringArray.Builder String;
    public Int64Array.Builder Integer;
    public DoubleArray.Builder Float;
    public BooleanArray.Builder Boolean;

    public ParamValueBuilder()
    {
        String = new();
        Integer = new();
        Float = new();
        Boolean = new();
    }

    public void Append(string value)
    {
        String.Append(value);
        Integer.AppendNull();
        Float.AppendNull();
        Boolean.AppendNull();
    }

    public void Append(long value)
    {
        String.AppendNull();
        Integer.Append(value);
        Float.AppendNull();
        Boolean.AppendNull();
    }

    public void Append(double value)
    {
        String.AppendNull();
        Integer.AppendNull();
        Float.Append(value);
        Boolean.AppendNull();
    }

    public void Append(bool value)
    {
        String.AppendNull();
        Integer.AppendNull();
        Float.AppendNull();
        Boolean.Append(value);
    }

    public void AppendNull()
    {
        String.AppendNull();
        Integer.AppendNull();
        Float.AppendNull();
        Boolean.AppendNull();
    }

    public void Append(Param parameter)
    {
        if (parameter.IsBoolean()) Append(parameter.AsBoolean());
        else if (parameter.IsDouble()) Append(parameter.AsDouble());
        else if (parameter.IsLong()) Append(parameter.AsLong());
        else if (parameter.IsString()) Append(parameter.AsString());
        else AppendNull();
    }

    public List<Field> ArrowType()
    {
        return new() {new Field("value", new StructType([
            new Field("string", new StringType(), true),
            new Field("integer", new Int64Type(), true),
            new Field("float", new DoubleType(), true),
            new Field("boolean", new BooleanType(), true),
        ]), true)};
    }

    public void Clear()
    {
        String.Clear();
        Integer.Clear();
        Float.Clear();
        Boolean.Clear();
    }

    public List<IArrowArray> Build()
    {
        List<IArrowArray> values = new(){new StructArray(ArrowType()[0].DataType, String.Length, [String.Build(), Integer.Build(), Float.Build(), Boolean.Build()], default)};
        Clear();
        return values;
    }

    public int Length => String.Length;
}

public class ParamBuilder : IArrowBuilder<Param>
{
    public StringArray.Builder Name;
    public StringArray.Builder AccessionCURIE;
    public ParamValueBuilder Value;
    public StringArray.Builder UnitCURIE;

    public int Length => Name.Length;

    public ParamBuilder()
    {
        Name = new();
        AccessionCURIE = new();
        Value = new();
        UnitCURIE = new();
    }

    public void AppendNull()
    {
        Name.AppendNull();
        AccessionCURIE.AppendNull();
        Value.AppendNull();
        UnitCURIE.AppendNull();
    }

    public void Append(Param param)
    {
        Name.Append(param.Name);
        if (param.AccessionCURIE != null) AccessionCURIE.Append(param.AccessionCURIE);
        else AccessionCURIE.AppendNull();
        Value.Append(param);
        if (param.UnitCURIE != null) UnitCURIE.Append(param.UnitCURIE);
        else UnitCURIE.AppendNull();
    }

    public List<Field> ArrowType()
    {
        return new(){
            new Field("param", new StructType([
            new Field("name", new StringType(), false),
            new Field("accession", new StringType(), true),
            new Field("value", Value.ArrowType()[0].DataType, false),
            new Field("unit", new StringType(), true),
        ]), true)};
    }

    public List<IArrowArray> Build()
    {
        List<IArrowArray> values = new() {
            new StructArray(
                ArrowType()[0].DataType,
                Name.Length,
                [Name.Build(), AccessionCURIE.Build(), Value.Build()[0], UnitCURIE.Build()],
                default
            )
        };

        return values;
    }

    public void Clear()
    {
        Name.Clear();
        AccessionCURIE.Clear();
        Value.Clear();
        UnitCURIE.Clear();
    }

    public void Visit(StructArray type)
    {
        throw new NotImplementedException();
    }
}

public class ParamListBuilder : IArrowBuilder<List<Param>>
{
    public ParamBuilder ValueBuilder { get; }
    private ArrowBuffer.Builder<int> ValueOffsetsBufferBuilder { get; }

    private ArrowBuffer.BitmapBuilder ValidityBufferBuilder { get; }
    public int NullCount { get; protected set; }

    public int Length => ValueOffsetsBufferBuilder.Length;

    public ParamListBuilder()
    {
        ValueBuilder = new();
        ValueOffsetsBufferBuilder = new();
        ValidityBufferBuilder = new();
        NullCount = 0;
        Append();
    }

    public void AppendNull()
    {
        ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);
        ValidityBufferBuilder.Append(false);
        NullCount++;
    }

    public void Append(List<Param> @params)
    {
        foreach(var par in @params)
        {
            ValueBuilder.Append(par);
        }
        Append();
    }

    public void Append()
    {
        ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);
        ValidityBufferBuilder.Append(true);
    }

    public List<Field> ArrowType()
    {
        return new (){
            new Field("parameters", new ListType(ValueBuilder.ArrowType()[0].DataType), true)
        };
    }

    public ListArray Build(MemoryAllocator? allocator = default)
    {
        ValueOffsetsBufferBuilder.Append(ValueBuilder.Length);
        ArrowBuffer validityBuffer = NullCount > 0
                                ? ValidityBufferBuilder.Build(allocator)
                                : ArrowBuffer.Empty;
        var dtype = ValueBuilder.ArrowType()[0];
        var dataType = new ListType(dtype.DataType);
        var values = ValueBuilder.Build()[0];
        var listy = new ListArray(
            dataType,
            Length - 1,
            ValueOffsetsBufferBuilder.Build(allocator), values,
            validityBuffer, NullCount, 0
        );
        return listy;
    }

    public List<IArrowArray> Build()
    {
        var list = Build(null);
        return [list];
    }

    public void Clear()
    {
        ValueBuilder.Clear();
        ValidityBufferBuilder.Clear();
        ValueOffsetsBufferBuilder.Clear();
    }
}

public class CustomBuilderFromParam : IArrowBuilder<Param>
{
    public string AccessionCURIE;
    public string Name;
    public string? FixedUnit;
    public ArrowType ValueType;
    IArrowArrayBuilder Value;

    public int Length => Value.Length;

    public StringArray.Builder? UnitValue;

    public CustomBuilderFromParam(string accessionCURIE, string name, ArrowType arrowType, string? fixedUnit=null, bool includeUnitValue=false)
    {
        AccessionCURIE = accessionCURIE;
        Name = name;
        ValueType = arrowType;
        FixedUnit = fixedUnit;
        if (fixedUnit != null && includeUnitValue) throw new InvalidOperationException("May only specify one of fixedUnit or includingUnitValue");
        UnitValue = includeUnitValue ? new StringArray.Builder() : null;
        switch(arrowType.TypeId)
        {
            case ArrowTypeId.Int64:
                {
                    Value = new Int64Array.Builder();
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    Value = new Int32Array.Builder();
                    break;
                }
            case ArrowTypeId.Double:
                {
                    Value = new DoubleArray.Builder();
                    break;
                }
            case ArrowTypeId.String:
                {
                    Value = new StringArray.Builder();
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    Value = new BooleanArray.Builder();
                    break;
                }
            default:
                throw new InvalidOperationException("CustomBuilderFromParam does not support " + arrowType.Name);
        }
    }

    public void AppendNull()
    {
        switch (ValueType.TypeId)
        {
            case ArrowTypeId.Int64:
                {
                    ((Int64Array.Builder)Value).AppendNull();
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    ((Int32Array.Builder)Value).AppendNull();
                    break;
                }
            case ArrowTypeId.Double:
                {
                    ((DoubleArray.Builder)Value).AppendNull();
                    break;
                }
            case ArrowTypeId.String:
                {
                    ((StringArray.Builder)Value).AppendNull();
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    ((BooleanArray.Builder)Value).AppendNull();
                    break;
                }
            default:
                throw new InvalidOperationException("CustomBuilderFromParam does not support " + ValueType.Name);
        }
    }

    public void Append(Param param)
    {
        if (param.IsNull())
        {
            AppendNull();
            return;
        }
        switch (ValueType.TypeId)
        {
            case ArrowTypeId.Int64:
                {
                    ((Int64Array.Builder)Value).Append(param.AsLong());
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    ((Int32Array.Builder)Value).Append((int)param.AsLong());
                    break;
                }
            case ArrowTypeId.Double:
                {
                    ((DoubleArray.Builder)Value).Append(param.AsDouble());
                    break;
                }
            case ArrowTypeId.String:
                {
                    ((StringArray.Builder)Value).Append(param.AsString());
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    ((BooleanArray.Builder)Value).Append(param.AsBoolean());
                    break;
                }
            default:
                throw new InvalidOperationException("CustomBuilderFromParam does not support " + ValueType.Name);
        }
    }

    public List<Field> ArrowType()
    {
        var fields = new List<Field>();
        var baseName = ColumnParam.Inflect(AccessionCURIE, Name, FixedUnit);
        switch (ValueType.TypeId)
        {
            case ArrowTypeId.Int64:
                {
                    fields.Add(new Field(baseName, new Int64Type(), true));
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    fields.Add(new Field(baseName, new Int32Type(), true));
                    break;
                }
            case ArrowTypeId.Double:
                {
                    fields.Add(new Field(baseName, new DoubleType(), true));
                    break;
                }
            case ArrowTypeId.String:
                {
                    fields.Add(new Field(baseName, new StringType(), true));
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    fields.Add(new Field(baseName, new BooleanType(), true));
                    break;
                }
            default:
                throw new InvalidOperationException("CustomBuilderFromParam does not support " + ValueType.Name);
        }
        if (UnitValue != null)
        {
            fields.Add(new Field(baseName + "_unit", new StringType(), true));
        }
        return fields;
    }

    public List<IArrowArray> Build()
    {
        var cols = new List<IArrowArray>();
        switch (ValueType.TypeId)
        {
            case ArrowTypeId.Int64:
                {
                    cols.Add(((Int64Array.Builder)Value).Build());
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    cols.Add(((Int32Array.Builder)Value).Build());
                    break;
                }
            case ArrowTypeId.Double:
                {
                    cols.Add(((DoubleArray.Builder)Value).Build());
                    break;
                }
            case ArrowTypeId.String:
                {
                    cols.Add(((StringArray.Builder)Value).Build());
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    cols.Add(((BooleanArray.Builder)Value).Build());
                    break;
                }
            default:
                throw new InvalidOperationException("CustomBuilderFromParam does not support " + ValueType.Name);
        }
        if (UnitValue != null) {
            cols.Add(UnitValue.Build());
        }
        return cols;
    }

    public void Clear()
    {
        switch (ValueType.TypeId)
        {
            case ArrowTypeId.Int64:
                {
                    ((Int64Array.Builder)Value).Clear();
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    ((Int32Array.Builder)Value).Clear();
                    break;
                }
            case ArrowTypeId.Double:
                {
                    ((DoubleArray.Builder)Value).Clear();
                    break;
                }
            case ArrowTypeId.String:
                {
                    ((StringArray.Builder)Value).Clear();
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    ((BooleanArray.Builder)Value).Clear();
                    break;
                }
            default:
                throw new InvalidOperationException("CustomBuilderFromParam does not support " + ValueType.Name);
        }
        UnitValue?.Clear();
    }
}

public class ParamVisitorCollection
{
    public List<CustomBuilderFromParam> ParamVisitors;
    public ParamListBuilder ParamList;
    public HashSet<string> Visited;

    public ParamVisitorCollection(List<CustomBuilderFromParam> paramVisitors)
    {
        ParamVisitors = paramVisitors;
        ParamList = new();
        Visited = new();
    }

    public void VisitParameters(List<Param> @params)
    {
        foreach (var par in @params)
        {
            foreach (var vis in ParamVisitors)
            {
                if (vis.AccessionCURIE == par.AccessionCURIE)
                {
                    if (Visited.Contains(par.AccessionCURIE)) continue;
                    vis.Append(par);
                    Visited.Add(par.AccessionCURIE);
                }
            }
        }
        foreach (var vis in ParamVisitors)
        {
            if (!Visited.Contains(vis.AccessionCURIE)) vis.AppendNull();
        }
        ParamList.Append(@params.Where((v) => v.AccessionCURIE == null || !Visited.Contains(v.AccessionCURIE)).ToList());
    }

    public virtual void AppendNull()
    {
        ParamList.AppendNull();
        foreach(var vis in ParamVisitors)
        {
            vis.AppendNull();
        }
    }
}
