using System;
using System.ComponentModel.DataAnnotations;
using System.IO;

using McMaster.Extensions.CommandLineUtils;

namespace BreweryDataGrabber
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var app = new CommandLineApplication();

            app.HelpOption();
            var api = app.Option("-k|--key <APIKEY>", "Api Key for beermapping.com", CommandOptionType.SingleValue);
            var path = app.Option("-f|--file <FILEPATH>", "Path to the data.csv file", CommandOptionType.SingleValue);
            var output = app.Option("-o|--output <OUTPUTPATH>", "Path to save the results.csv to", CommandOptionType.SingleOrNoValue);
            var namecol = app.Option("-c|--column <BREWERYNAMECOLUMN>", 
                "Column number that holds the brewery names. Defaults to the second column", CommandOptionType.SingleOrNoValue);

            app.OnExecuteAsync(async cancellationToken =>
            {
                var key = api.Value();
                if(key is null)
                {
                    await app.Error.WriteLineAsync("No API key was provided. Use -k|--key <API KEY> to specify an API key.");
                    return;
                }

                var input = path.Value();
                if (input is null)
                {
                    await app.Error.WriteLineAsync("No input file provided. Use -f|--file <FILE PAHT> to specify an input file.");
                    return;
                }

                if(!File.Exists(input))
                {
                    await app.Error.WriteLineAsync($"The file {Path.GetFileName(input)} was not found.");
                    return;
                }

                var outFile = output.Value();

                if (outFile is null)
                    outFile = Path.Join(Path.GetDirectoryName(input), "results.csv");

                var ncolraw = namecol.Value();
                int ncol = 1;

                if(!int.TryParse(ncolraw, out ncol))
                {
                    await app.Error.WriteLineAsync($"The value {ncolraw} is not a valid integer.");
                    return;
                }

                await DataGrabber.Execute(cancellationToken, app, input, outFile, key, --ncol);
            });

            app.ExecuteAsync(args).GetAwaiter().GetResult();
        }
    }
}
