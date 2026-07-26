namespace MzPeakTests;

using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Reader;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;
using Xunit.Sdk;

public class ArchiveTest
{
    IMZPeakArchiveStorage PointArchive;
    IMZPeakArchiveStorage ChunkArchive;

    public ArchiveTest()
    {
        string fileName = "small.mzpeak";
        string baseDirectory = AppContext.BaseDirectory; // Gets the directory where tests are running
        string fullPath = Path.Combine(baseDirectory, fileName);
        PointArchive = new LocalZipArchive(fullPath);
        fileName = "small.chunked.mzpeak";
        baseDirectory = AppContext.BaseDirectory; // Gets the directory where tests are running
        fullPath = Path.Combine(baseDirectory, fileName);
        ChunkArchive = new LocalZipArchive(fullPath);
    }

    [Fact]
    public void RawZipArchive_LoadIndex()
    {
        var index = PointArchive.FileIndex();
        Assert.Equal(10, index.Files.Count);
        Assert.Equal(11, PointArchive.FileNames().Count);
    }

    async Task ExerciseLoadSpectrum(IMZPeakArchiveStorage archiveStorage, BufferFormat bufferFormat)
    {
        var meta = archiveStorage.OpenNamespace(EntityType.Spectrum);
        Assert.NotNull(meta);
        var metaReader = new SpectrumMetadataReader(meta);
        var models = metaReader.GetSpacingModelIndex();
        Assert.Equal(14, models.Count);

        var reader = archiveStorage.SpectrumData();
        Assert.NotNull(reader);
        var dataReader = new DataArraysReader(reader, BufferContext.Spectrum)
        {
            SpacingModels = models
        };

        Assert.Equal(bufferFormat, dataReader.Metadata.Format);
        Assert.Single(dataReader.RowGroupIndex);
        Assert.True(dataReader.ArrayIndex.Entries.All((e) => e.SchemaIndex != null));
        var data = await dataReader.ReadForIndex(10);
        Assert.NotNull(data);

        var it = dataReader.Enumerate();
        await foreach ((ulong i, StructArray chunk) in it)
        {
            var dtype = (StructType)chunk.Data.DataType;
            foreach (var (f, arr) in dtype.Fields.Zip(chunk.Fields))
            {
                Assert.Equal(0, arr.NullCount);
                Assert.NotEqual(0, arr.Length);
            }
        }
    }

    [Fact]
    public async Task RawZipArchive_LoadSpectrumPoint()
    {
        await ExerciseLoadSpectrum(PointArchive, BufferFormat.Point);
    }

    [Fact]
    public async Task RawZipArchive_LoadSpectrumChunk()
    {
        await ExerciseLoadSpectrum(ChunkArchive, BufferFormat.ChunkValues);
    }

    [Fact]
    public void RawZipArchive_LoadSpectrumIndex()
    {
        var reader = PointArchive.SpectrumData();
        Assert.NotNull(reader);

        var kvMeta = reader.ParquetReader.FileMetaData.KeyValueMetadata;
        var arrayIndexText = kvMeta["spectrum_array_index"];
        Assert.NotNull(arrayIndexText);
        var arrayIndex = JsonSerializer.Deserialize<ArrayIndex>(arrayIndexText);
        Assert.NotNull(arrayIndex);
        Assert.Equal("point", arrayIndex.Prefix);
        Assert.Equal(BufferFormat.Point, arrayIndex.Entries[0].BufferFormat);
        Assert.Equal(BufferFormat.Point, arrayIndex.Entries[1].BufferFormat);
    }

    void ExerciseArchiveSpectrumMetadata(IMZPeakArchiveStorage archiveStorage)
    {
        var stream = archiveStorage.OpenNamespace(EntityType.Spectrum);
        Assert.NotNull(stream);
        var meta = new SpectrumMetadataReader(stream);
        Assert.NotNull(meta);
        Assert.NotNull(meta.SpectrumMetadata);
        var chunk = ((StructArray)meta.SpectrumMetadata.Array(0)).AsRecordBatch();
        var col = chunk.Column("index");
        Assert.NotNull(col);
        Assert.Equal(48, col.Length);
        var idxArray = (UInt64Array)col;
        Assert.NotNull(idxArray.GetValue(0));
        Assert.Equal(0ul, idxArray.GetValue(0));

        col = ((StructArray?)meta.ScanMetadata?.Array(0))?.AsRecordBatch().Column("parameters");
        Assert.NotNull(col);
        var builder = new ParamListVisitor();
        builder.Visit(col);
        var paramsList = builder.ParamsLists;
        var k = 0;
        foreach (var pars in paramsList)
        {
            k += pars.Count;
        }
        Assert.True(k > 0);

        var vals = meta.MZRange();
        Assert.NotNull(vals);
        Assert.NotEqual(double.PositiveInfinity, vals.Value.Item1);
        Assert.NotEqual(double.NegativeInfinity, vals.Value.Item2);
    }

    async Task ExerciseArchiveGetDataIter(IMZPeakArchiveStorage archiveStorage)
    {
        var reader = archiveStorage.SpectrumData();
        ulong i = 0;

        Assert.NotNull(reader);

        var dataReader = new DataArraysReader(reader, BufferContext.Spectrum);
        var iter = dataReader.Enumerate();

        List<ulong> profileSpectrumIdx = [
            0,
            1,
            7,
            8,
            14,
            15,
            21,
            22,
            28,
            29,
            34,
            35,
            41,
            42
        ];

        await foreach (var pair in iter)
        {
            if (pair.Item1 > 10) break;
            Assert.Equal(profileSpectrumIdx[(int)i++], pair.Item1);
            Assert.NotEqual(0, pair.Item2.Length);
        }
        await iter.Seek(21);
        i = 6;
        await foreach (var pair in iter)
        {
            Assert.Equal(profileSpectrumIdx[(int)i++], pair.Item1);
            Assert.NotEqual(0, pair.Item2.Length);
        }
        Assert.Equal(profileSpectrumIdx.Count, (int)i);
    }

    [Fact]
    public void RawZipArchive_SpectrumMetadata()
    {
        ExerciseArchiveSpectrumMetadata(PointArchive);
    }

    [Fact]
    public async Task RawZipArchive_Point_GetDataIter()
    {
        await ExerciseArchiveGetDataIter(PointArchive);
    }

    [Fact]
    public async Task RawZipArchive_Chunked_GetDataIter()
    {
        await ExerciseArchiveGetDataIter(ChunkArchive);
    }

    [Fact]
    public void RawZipArchive_Http_SpectrumMetadata()
    {
        try
        {
            var stream = new HttpStream("http://localhost:8030/small.mzpeak");
            Assert.True(stream.CanRead);
            var header = new byte[4];
            stream.ReadExactly(header);
            Assert.True(BaseZipArchive.IsZipArchiveHeader(header));
            Assert.Equal(4, stream.Position);
            stream.Seek(0, SeekOrigin.Begin);
            header = new byte[4];
            stream.ReadExactly(header);
            Assert.True(BaseZipArchive.IsZipArchiveHeader(header));
            Assert.Equal(4, stream.Position);

            stream.Seek(0, SeekOrigin.Begin);
            var zipStream = new ZipArchiveStream<HttpStream>(stream);

            Assert.NotEmpty(zipStream.FileIndex().Files);
        } catch
        {
            Console.Error.WriteLine("No HTTP server running...");
            return;
        }
        var archive = new HttpZipArchive("http://localhost:8030/small.mzpeak");
        ExerciseArchiveSpectrumMetadata(archive);
    }

    [Fact]
    public async Task RawZipArchive_Http_GetDataIter()
    {
        try
        {
            var con = new HttpStream("http://localhost:8030/small.mzpeak");
        }
        catch
        {
            Console.Error.WriteLine("No HTTP server running...");
            return;
        }
        var archive = new HttpZipArchive("http://localhost:8030/small.mzpeak");
        await ExerciseArchiveGetDataIter(archive);
    }

}

public class ParamTest
{
    [Fact]
    public void Param_FromJson()
    {
        var msg = "{\"name\": \"foobar\", \"value\": null}";
        var param = JsonSerializer.Deserialize<Param>(msg);
        Assert.NotNull(param);

        msg = "{\"name\": \"foobar\", \"value\": 150.1}";
        param = JsonSerializer.Deserialize<Param>(msg);
        Assert.NotNull(param);
        Assert.True(param.IsDouble());
        Assert.False(param.IsLong());
        Assert.Equal(150, param.AsLong());

        msg = "{\"name\": \"foobar\", \"value\": \"bazbang\"}";
        param = JsonSerializer.Deserialize<Param>(msg);
        Assert.NotNull(param);
        Assert.True(param.IsString());
    }

    [Fact]
    public void Param_ToJson()
    {
        var param = new Param("foobar", "UNK:000", true, "UO:0");
        var msg = JsonSerializer.Serialize(param);
        var expected = "{\"name\":\"foobar\",\"accession\":\"UNK:000\",\"value\":true,\"unit\":\"UO:0\"}";
        Assert.Equal(expected, msg);
    }
}
