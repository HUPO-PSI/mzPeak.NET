using MZPeak;
using MZPeak.Storage;
using System.CommandLine;
using System.Threading.Tasks;

namespace MZPeakCliConverter;

internal class Program
{
    static void Main(string[] args)
    {
        Option<FileInfo> fileOption = new("--file");

        RootCommand rootCommand = new("Demo application for mzPeak .NET")
        {
            CreateReadCommand()
        };

        var opts = rootCommand.Parse(args);
        opts.Invoke();
    }

    static Command CreateReadCommand()
    {
        var cmd = new Command("read", "Read an existing mzPeak file");
        Argument<FileInfo> filePath = new Argument<FileInfo>("file").AcceptExistingOnly();
        cmd.Arguments.Add(filePath);
        cmd.SetAction(parseResult =>
        {
            var fp = parseResult.GetValue(filePath);
            if (fp == null) {
                parseResult.RootCommandResult.AddError("File argument was missing");
            }
            else
            {
                ReadFile(fp);
            }
        });
        return cmd;
    }

    static async Task TranscodeFile(FileInfo sourceFile, FileInfo destinationFile)
    {
        var reader = new MZPeak.Reader.MzPeakReader(sourceFile.FullName);
        var spectrumArrays = reader.SpectrumDataReaderMeta?.ArrayIndex;
        if (spectrumArrays == null)
        {
            Console.WriteLine("Cannot transcode a file without spectra yet");
            return;
        }
        var writer = new MZPeak.Writer.MZPeakWriter(
            new ZipStreamArchiveWriter<FileStream>(File.OpenWrite(destinationFile.FullName)),
            spectrumArrays
        );

        writer.FileDescription = reader.FileDescription;
        writer.InstrumentConfigurations = reader.InstrumentConfigurations;
        writer.DataProcessingMethods = reader.DataProcessingMethods;
        writer.Samples = reader.Samples;
        writer.Run = reader.Run;
        writer.Softwares = reader.Softwares;

        await foreach(var(descr, data) in reader.EnumerateSpectraAsync())
        {
            var index = writer.CurrentSpectrum;
            var (spacingModel, auxArrays) = writer.AddSpectrumData(index, data.Fields.Skip(1));
            writer.AddSpectrum(
                descr.Id,
                descr.Time,
                descr.DataProcessingRef,
                spacingModel?.Coefficients,
                descr.Parameters,
                auxArrays
            );
            foreach(var scan in descr.Scans)
            {
                writer.AddScan(
                    index,
                    scan.InstrumentConfigurationRef,
                    scan.IonMobility,
                    scan.IonMobilityTypeCURIE,
                    scan.Parameters
                );
            }
            foreach(var precursor in descr.Precursors)
            {

            }
        }
    }

    static void ReadFile(FileInfo fileInfo)
    {
        Console.WriteLine($"Reading {fileInfo}");
        var reader = new MZPeak.Reader.MzPeakReader(fileInfo.FullName);
        Console.WriteLine($"{reader.SpectrumCount} spectra detected, {reader.ChromatogramCount} chromatograms detected");
        Console.WriteLine($"Spectrum storage format = {reader.SpectrumDataFormat}");

    }
}
