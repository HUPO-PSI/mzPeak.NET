set script-interpreter := ['bash', '-eu']

test:
    dotnet test --logger "console;verbosity=detailed"

read INPATH:
    dotnet run --project "MZPeakNet.AppTest" -- --verbose read {{INPATH}}

small:
    dotnet run --project "MZPeakNet.AppTest" -- --verbose thermo "./TestData/small.RAW" test.mzpeak
    dotnet run --project "MZPeakNet.AppTest" -- --verbose thermo -c -u "./TestData/small.RAW" test.chunked.mzpeak

[positional-arguments]
convert-thermo INPATH OUTPATH *args='':
    dotnet run --project "MZPeakNet.AppTest" -- --verbose thermo {{args}} {{INPATH}} {{OUTPATH}}


coverage:
    dotnet test -p:CollectCoverage=true -p:CoverletOutputFormat=cobertura
    reportgenerator -reports:"./MZPeakNetTests/coverage.cobertura.xml" -targetdir:"coveragereport" -reporttypes:Html