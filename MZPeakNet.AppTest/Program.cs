using MZPeak;
using MZPeak.Storage;
using System.CommandLine;
using Microsoft.Extensions.Logging;

namespace MZPeakCliConverter;

internal class Program
{
    static ILogger? Logger = null;

    static void Main(string[] args)
    {
        ConfigureLogging();
#if DEBUG
        Logger?.LogInformation("Running Debug Mode");
#endif
        RootCommand rootCommand = new("Demo application for mzPeak .NET")
        {
            CreateReadCommand(),
            CreateTranscodeCommand(),
        };

        var opts = rootCommand.Parse(args);
        opts.Invoke();
    }

    static void ConfigureLogging()
    {
        var loggerFactory = LoggerFactory.Create(builder => {
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "HH:mm:ss";
            });
        });

        MZPeak.Util.LoggingConfig.ConfigureLogging(loggerFactory);
        Logger = loggerFactory.CreateLogger("MZPeakNet.App");
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
                ReadFile(fp).Wait();
            }
        });
        return cmd;
    }

    static Command CreateTranscodeCommand()
    {
        var cmd = new Command("transcode", "Read an existing mzPeak file and write another mzPeak file");
        Argument<FileInfo> filePath = new Argument<FileInfo>("file").AcceptExistingOnly();
        cmd.Arguments.Add(filePath);

        Argument<FileInfo> outPath = new Argument<FileInfo>("out").AcceptLegalFilePathsOnly();
        cmd.Arguments.Add(outPath);

        cmd.SetAction(parseResult =>
        {
            var fp = parseResult.GetRequiredValue(filePath);
            var outp = parseResult.GetRequiredValue(outPath);
            TranscodeFile(fp, outp).Wait();
        });

        return cmd;
    }

    static async Task TranscodeFile(FileInfo sourceFile, FileInfo destinationFile)
    {
        var reader = new MZPeak.Reader.MzPeakReader(sourceFile.FullName);
        var spectrumArrays = reader.SpectrumDataReaderMeta?.ArrayIndex;
        if (spectrumArrays == null)
        {
            Logger?.LogError("Cannot transcode a file without spectra yet");
            return;
        }
        using (var fileStream = File.Create(destinationFile.FullName))
        {
            var writerStorage = new ZipStreamArchiveWriter<FileStream>(fileStream);

            var writer = new MZPeak.Writer.MZPeakWriter(
                writerStorage,
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
                Logger?.LogInformation($"Writing {descr.Index} = {descr.Id} with {data.Length} points");
                var index = writer.CurrentSpectrum;
                var (spacingModel, auxArrays) = writer.AddSpectrumData(index, data.Fields.Skip(1), descr.IsProfile);
                writer.AddSpectrum(
                    descr.Id,
                    descr.Time,
                    descr.DataProcessingRef,
                    spacingModel?.Coefficients ?? new(),
                    descr.Parameters,
                    auxArrays
                );
                foreach(var scan in descr.Scans)
                {
                    writer.AddScan(
                        index,
                        scan.InstrumentConfigurationRef,
                        scan.Parameters,
                        scan.IonMobility,
                        scan.IonMobilityTypeCURIE
                    );
                }
                foreach(var precursor in descr.Precursors)
                {
                    writer.AddPrecursor(
                        index,
                        precursor.PrecursorIndex,
                        precursor.PrecursorId,
                        precursor.IsolationWindowParameters,
                        precursor.ActivationParameters
                    );
                }
                foreach(var selectedIon in descr.SelectedIons)
                {
                    writer.AddSelectedIon(
                        index,
                        selectedIon.PrecursorIndex,
                        selectedIon.Parameters,
                        selectedIon.IonMobility,
                        selectedIon.IonMobilityTypeCURIE
                    );
                }
            }
            writer.FlushSpectrumData();
            await foreach(var(descr, data) in reader.EnumerateChromatogramsAsync())
            {
                Logger?.LogInformation($"Writing {descr.Index} = {descr.Id} with {data.Length} points");
                var index = writer.CurrentChromatogram;
                var auxArrays = writer.AddChromatogramData(index, data.Fields.Skip(1));
                writer.AddChromatogram(
                    descr.Id,
                    descr.DataProcessingRef,
                    descr.Parameters,
                    auxArrays
                );

            }
            writer.Close();
        }

    }

    static async Task ReadFile(FileInfo fileInfo)
    {
        Logger?.LogInformation($"Reading {fileInfo}");
        var reader = new MZPeak.Reader.MzPeakReader(fileInfo.FullName);
        Logger?.LogInformation($"{reader.SpectrumCount} spectra detected, {reader.ChromatogramCount} chromatograms detected");
        Logger?.LogInformation($"Spectrum storage format = {reader.SpectrumDataFormat}");

        var isProfile = 0;
        var isCentroid = 0;
        var i = 0;
        await foreach(var (descr, spec) in reader.EnumerateSpectraAsync())
        {
            i++;
            if (i % 1000 == 0) Logger?.LogInformation($"{i} spectra read...");
            isProfile += descr.IsProfile ? 1 : 0;
            isCentroid += descr.IsCentroid ? 1 : 0;
        }

        Logger?.LogInformation($"{isProfile} profile spectra, {isCentroid} centroid spectra");
    }
}
