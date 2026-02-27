using System.CommandLine;
using Microsoft.Extensions.Logging;

using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.RandomAccessReaderPlugin;
using ThermoFisher.CommonCore.RawFileReader;

using MZPeak.Storage;
using MZPeak.Thermo;
using MZPeak.Compute;
using MZPeak.Metadata;
using MZPeak.ControlledVocabulary;
using MZPeak.Writer.Data;
using System.ComponentModel;
using ThermoFisher.CommonCore.Data;


namespace MZPeakCliConverter;

internal class Program
{
    static ILogger? Logger = null;

    static void Main(string[] args)
    {
        var startTime = DateTime.Now;
        ConfigureLogging();
#if DEBUG
        Logger?.LogInformation("Running Debug Mode");
#endif
        RootCommand rootCommand = new("Demo application for mzPeak .NET")
        {
            CreateReadCommand(),
            CreateReadSpectrum(),
            CreateTranscodeCommand(),
            CreateThermoTranslateCommand(),
        };

        var opts = rootCommand.Parse(args);
        opts.Invoke();

        var elapsed = DateTime.Now - startTime;
        Logger?.LogInformation($"{elapsed:c} elapsed");
    }

    static void ConfigureLogging()
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
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
            if (fp == null)
            {
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

    static Command CreateThermoTranslateCommand()
    {
        var cmd = new Command("thermo", "Read a Thermo RAW file and write mzPeak file");
        Argument<FileInfo> filePath = new Argument<FileInfo>("file").AcceptExistingOnly();
        cmd.Arguments.Add(filePath);

        Option<bool> nullMarking = new Option<bool>("--use-null-marking", "-u");
        cmd.Options.Add(nullMarking);
        Option<bool> useChunked = new Option<bool>("--use-chunking", "-c");
        cmd.Options.Add(useChunked);


        Argument<FileInfo> outPath = new Argument<FileInfo>("out").AcceptLegalFilePathsOnly();
        cmd.Arguments.Add(outPath);

        cmd.SetAction(parseResult =>
        {
            var fp = parseResult.GetRequiredValue(filePath);
            var outp = parseResult.GetRequiredValue(outPath);
            var nullMark = parseResult.GetValue(nullMarking);
            var chunked = parseResult.GetValue(useChunked);
            ThermoTranslate(fp, outp, nullMark, chunked);
        });

        return cmd;
    }

    static Command CreateReadSpectrum()
    {
        var cmd = new Command("spectrum", "Read a spectrum from an mzPeak file");
        Argument<FileInfo> filePath = new Argument<FileInfo>("file").AcceptExistingOnly();
        cmd.Arguments.Add(filePath);
        Argument<ulong> indexArg = new Argument<ulong>("index");
        cmd.Arguments.Add(indexArg);
        cmd.SetAction(parseResult =>
        {
            var fp = parseResult.GetValue(filePath);
            var idx = parseResult.GetValue(indexArg);
            if (fp == null)
            {
                parseResult.RootCommandResult.AddError("File argument was missing");
            }
            else
            {

                ReadSpectrum(fp, idx).Wait();
            }
        });
        return cmd;
    }

    static async Task ReadSpectrum(FileInfo sourceFile, ulong spectrumIndex)
    {
        var reader = new MZPeak.Reader.MzPeakReader(sourceFile.FullName);
        var spec = await reader.GetSpectrumData(spectrumIndex);
        if (spec != null)
        {
            Compute.PrettyPrint(spec);
            Console.WriteLine($"{spec.Length} points");
        }
    }

    static void ThermoTranslate(FileInfo sourceFile, FileInfo destinationFile, bool useNullMarking=false, bool useChunked=false)
    {
        var readerManager = RawFileReaderAdapter.RandomAccessThreadedFileFactory(sourceFile.FullName, RandomAccessFileManager.Instance);
        var accessor = readerManager.CreateThreadAccessor();
        if (!accessor.SelectMsData())
        {
            Logger?.LogWarning("No MS data detected! Exiting Early!");
            return;
        }
        accessor.IncludeReferenceAndExceptionData = true;

        using (var fileStream = File.Create(destinationFile.FullName))
        {
            var writerStorage = new ZipStreamArchiveWriter<FileStream>(fileStream);

            var writer = new ThermoMZPeakWriter(
                writerStorage,
                spectrumPeakArrayIndex: ThermoMZPeakWriter.PeakArrayIndex(true, false),
                useChunked: useChunked,
                includeNoise: true
            );

            if (useNullMarking)
            {
                Logger?.LogInformation("Using null marking");
                foreach(var e in writer.SpectrumArrayIndex.EntriesFor(ArrayType.MZArray).Where(e => e.BufferFormat == BufferFormat.Point || e.BufferFormat == BufferFormat.ChunkValues))
                    e.Transform = NullInterpolation.NullInterpolateCURIE;
                foreach (var e in writer.SpectrumArrayIndex.EntriesFor(ArrayType.IntensityArray).Where(e => e.BufferFormat == BufferFormat.Point || e.BufferFormat == BufferFormat.ChunkSecondary))
                    e.Transform = NullInterpolation.NullZeroCURIE;
            }

            writer.InitializeHelper(accessor);

            writer.Samples.Add(writer.ConversionHelper.GetSample(accessor));
            writer.FileDescription = writer.ConversionHelper.GetFileDescription(accessor);

            writer.Run.DefaultSourceFileId = "RAW1";
            writer.Run.StartTime = accessor.FileHeader.CreationDate;
            writer.Run.Id = accessor.FileName;

            var startScan = accessor.RunHeader.FirstSpectrum;
            var lastScan = accessor.RunHeader.LastSpectrum;

            for (var scanNumber = startScan; scanNumber < lastScan; scanNumber++)
            {
                var scanFilter = accessor.GetFilterForScanNumber(scanNumber);
                var segments = accessor.GetSegmentedScanFromScanNumber(scanNumber);
                var statistics = accessor.GetScanStatsForScanNumber(scanNumber);
                var time = accessor.RetentionTimeFromScanNumber(scanNumber);

                if (scanNumber % 1000 == 0)
                {
                    Logger?.LogInformation(
                        $"Writing {scanNumber} with {segments.PositionCount} points"
                    );
                }

                var (spacingModel, auxArrays) = writer.AddSpectrumData(
                    writer.CurrentSpectrum,
                    segments,
                    statistics);

                var packets = accessor.GetAdvancedPacketData(scanNumber);
                if (packets.NoiseData != null && packets.NoiseData.Length > 0)
                {
                    writer.AddNoisePacketData(writer.CurrentSpectrum, packets.NoiseData);
                }

                if (!statistics.IsCentroidScan)
                {
                    auxArrays.AddRange(
                        writer.AddSpectrumPeakData(
                            writer.CurrentSpectrum,
                            accessor.GetCentroidStream(scanNumber, true)
                        )
                    );
                }

                var key = writer.AddSpectrum(
                    scanNumber,
                    time,
                    scanFilter,
                    statistics,
                    spacingModel?.Coefficients,
                    auxiliaryArrays: auxArrays);

                var (precursorProps, acquisitionProperties) = writer.ExtractPrecursorAndTrailerMetadata(
                    scanNumber,
                    accessor,
                    scanFilter,
                    statistics
                );

                writer.AddScan(
                    key,
                    scanNumber,
                    time,
                    scanFilter,
                    statistics,
                    acquisitionProperties
                );

                if (precursorProps != null)
                {
                    writer.AddPrecursor(
                        key,
                        precursorProps
                    );
                    writer.AddSelectedIon(
                        key,
                        precursorProps
                    );
                }
            }

            var (traceInfo, chromArrays) = writer.ConversionHelper.ReadSummaryTrace(TraceType.TIC, accessor);
            writer.AddChromatogramData(writer.CurrentChromatogram, chromArrays);
            writer.AddChromatogram(
                traceInfo.Id,
                null,
                traceInfo.Parameters,
                traceInfo.AuxiliaryArrays
            );

            (traceInfo, chromArrays) = writer.ConversionHelper.ReadSummaryTrace(TraceType.BasePeak, accessor);
            writer.AddChromatogramData(writer.CurrentChromatogram, chromArrays);
            writer.AddChromatogram(
                traceInfo.Id,
                null,
                traceInfo.Parameters,
                traceInfo.AuxiliaryArrays
            );

            Logger?.LogInformation("Writing traces");

            foreach(var log in writer.ConversionHelper.StatusLogs(accessor))
            {
                (traceInfo, var traceArrays) = log.AsChromatogramInfo();

                var auxArrays = writer.AddChromatogramData(writer.CurrentChromatogram, traceArrays);
                writer.AddChromatogram(
                    traceInfo.Id,
                    null,
                    traceInfo.Parameters,
                    auxArrays
                );
            }

            if (accessor.GetInstrumentCountOfType(Device.Pda) > 0)
            {
                Logger?.LogInformation("Reading PDA spectra");
                accessor.SelectInstrument(Device.Pda, 1);

                for(var i = accessor.RunHeader.FirstSpectrum; i < accessor.RunHeader.LastSpectrum; i++)
                {
                    var scan = accessor.GetSimplifiedScan(i);
                    Console.WriteLine($"{scan.Masses.Length}");
                }
            }
            if (accessor.GetInstrumentCountOfType(Device.UV) > 0)
            {
                Logger?.LogInformation("Reading UV spectra");
                accessor.SelectInstrument(Device.UV, 1);

                for (var i = accessor.RunHeader.FirstSpectrum; i < accessor.RunHeader.LastSpectrum; i++)
                {
                    var scan = accessor.GetSimplifiedScan(i);
                    Console.WriteLine($"{scan.Masses.Length}");
                }
            }

            Logger?.LogInformation("Closing writer...");
            writer.Close();
        }

        destinationFile.Refresh();
        Logger?.LogInformation($"Wrote {destinationFile.Length / 1000000.0} MB");
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

            await foreach (var (descr, data) in reader.EnumerateSpectraAsync())
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
                foreach (var scan in descr.Scans)
                {
                    writer.AddScan(
                        index,
                        scan.InstrumentConfigurationRef,
                        scan.Parameters,
                        scan.IonMobility,
                        scan.IonMobilityTypeCURIE,
                        scanWindows: scan.ScanWindows?.Select(w => w.AsParamList()).ToList()
                    );
                }
                foreach (var precursor in descr.Precursors)
                {
                    writer.AddPrecursor(
                        index,
                        precursor.PrecursorIndex,
                        precursor.PrecursorId,
                        precursor.IsolationWindowParameters,
                        precursor.ActivationParameters
                    );
                }
                foreach (var selectedIon in descr.SelectedIons)
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
            await foreach (var (descr, data) in reader.EnumerateChromatogramsAsync())
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
        destinationFile.Refresh();
        Logger?.LogInformation($"Wrote {destinationFile.Length / 1000000.0} MB");
    }

    static async Task ReadFile(FileInfo fileInfo)
    {
        Logger?.LogInformation($"Reading {fileInfo}");
        var reader = new MZPeak.Reader.MzPeakReader(fileInfo.FullName);
        Logger?.LogInformation($"{reader.SpectrumCount} spectra detected, {reader.ChromatogramCount} chromatograms detected");
        Logger?.LogInformation($"Spectrum storage format = {reader.SpectrumDataFormat}");
        if (reader.HasWavelengthData)
        {
            Logger?.LogInformation($"Wavelength spectrum count {reader.WavelengthSpectrumCount} in format {reader.WavelengthSpectrumDataFormat}");
        }

        var isProfile = 0;
        var isCentroid = 0;
        var i = 0;
        await foreach (var (descr, spec) in reader.EnumerateSpectraAsync())
        {
            i++;
            if (i % 1000 == 0) Logger?.LogInformation($"{i} spectra read...");
            isProfile += descr.IsProfile ? 1 : 0;
            isCentroid += descr.IsCentroid ? 1 : 0;
        }

        Logger?.LogInformation($"{isProfile} profile spectra, {isCentroid} centroid spectra");
    }
}
