using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

using McMaster.Extensions.CommandLineUtils;

namespace BreweryDataGrabber
{
    public static class DataGrabber
    {
        private static readonly HttpClient _client = new HttpClient();
        private const string URL_BASE_ID = "http://beermapping.com/webservice/";

        public static async Task Execute(CancellationToken cancellationToken, CommandLineApplication app, string input, string output,
            string apikey, int namecol)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var locations = await ReadFileDataAsync(cancellationToken, input, namecol);

            cancellationToken.ThrowIfCancellationRequested();

            var ids = await GetBreweryIdsAsync(cancellationToken, locations.Item1, apikey, app);

            cancellationToken.ThrowIfCancellationRequested();

            var coords = await GetGlobalCoordsAsync(cancellationToken, ids, apikey, app);

            cancellationToken.ThrowIfCancellationRequested();

            await SaveResultsAsync(cancellationToken, output, coords, locations.Item2, locations.Item3, app);
        }

        private static async Task<(HashSet<string>, ConcurrentDictionary<string, int>, List<string>)> ReadFileDataAsync(CancellationToken token, string file, int namecol)
        {
            token.ThrowIfCancellationRequested();

            using FileStream fs = new(file, FileMode.Open, FileAccess.Read, FileShare.Read);
            using StreamReader sr = new(fs);
            var csv = await sr.ReadToEndAsync();

            token.ThrowIfCancellationRequested();

            ConcurrentDictionary<string, int> locationCounts = new();
            List<string> order = new();
            HashSet<string> locations = new();
            int c = 0;
            foreach (var line in csv.Split("\n"))
            {
                token.ThrowIfCancellationRequested();

                if (c++ == 0) continue;
                var parts = line.Split(",");
                if(parts.Length > namecol)
                {
                    var brewery = parts[namecol];
                    if (locations.Add(brewery))
                        _ = locationCounts.TryAdd(brewery, 1);
                    else
                        locationCounts[brewery]++;

                    order.Add(brewery);
                }
            }

            token.ThrowIfCancellationRequested();

            return (locations, locationCounts, order);
        }

        private static async Task<ConcurrentDictionary<string, int>> GetBreweryIdsAsync(CancellationToken token, HashSet<string> locations,
            string apiKey, CommandLineApplication app)
        {
            token.ThrowIfCancellationRequested();

            ConcurrentDictionary<string, int> ids = new();

            var baseurl = $"locquery/{apiKey}/";

            _client.BaseAddress = new(URL_BASE_ID);

            token.ThrowIfCancellationRequested();

            foreach (var l in locations)
            {
                var str = l.ToLower();
                str = Regex.Replace(str, @"[^a-z]0-9\s-]", ""); // Remove all non valid chars          
                str = Regex.Replace(str, @"\s+", " ").Trim(); // convert multiple spaces into one space  
                str = Regex.Replace(str, @"\s", "+"); // //Replace spaces by dashes

                str = str.Replace(".", " ");

                var request = await _client.GetAsync($"{baseurl}{str}", token);

                if(request.IsSuccessStatusCode)
                {
                    token.ThrowIfCancellationRequested();

                    var raw = await request.Content.ReadAsStringAsync();

                    var idString = raw[(raw.IndexOf("<id>") + 4)..];
                    idString = idString[..idString.IndexOf("</id>")];

                    if (int.TryParse(idString, out var id))
                    {
                        ids[l] = id;
                        Console.WriteLine($"Got {id} for {l}");
                    }
                    else
                        await app.Error.WriteLineAsync($"Failed to get ID for {l}");
                }
                else
                {
                    await app.Error.WriteLineAsync($"Failed a request for {l} at {baseurl}/{l}, skipping ...");   
                }

                await Task.Delay(TimeSpan.FromSeconds(.25));
            }

            token.ThrowIfCancellationRequested();

            return ids;
        }

        private static async Task<ConcurrentDictionary<string, LatLng>> GetGlobalCoordsAsync(CancellationToken token, ConcurrentDictionary<string, int> ids,
            string apiKey, CommandLineApplication app)
        {
            token.ThrowIfCancellationRequested();

            ConcurrentDictionary<string, LatLng> coords = new();

            var baseurl = $"locmap/{apiKey}/";

            token.ThrowIfCancellationRequested();

            foreach (var id in ids)
            {
                if(id.Value == 0)
                {
                    Console.WriteLine($"ID for {id.Key} is 0, skipping lat/lng grab ...");
                    continue;
                }

                token.ThrowIfCancellationRequested();

                var request = await _client.GetAsync($"{baseurl}{id.Value}", token);

                if (request.IsSuccessStatusCode)
                {
                    token.ThrowIfCancellationRequested();

                    var raw = await request.Content.ReadAsStringAsync();

                    string rawlat = raw[(raw.IndexOf("<lat>") + 5)..];
                    rawlat = rawlat[..rawlat.IndexOf("</lat>")];

                    string rawlng = raw[(raw.IndexOf("<lng>") + 5)..];
                    rawlng = rawlng[..rawlng.IndexOf("</lng>")];

                    token.ThrowIfCancellationRequested();

                    if (double.TryParse(rawlat, out var lat) && double.TryParse(rawlng, out var lng))
                    {
                        coords[id.Key] = new()
                        {
                            Latitude = lat,
                            Longitude = lng
                        };

                        Console.WriteLine($"Got lat/lng {lat}/{lng} for {id.Key}");
                    }
                    else
                        await app.Error.WriteLineAsync($"Failed to get lat/lng for {id.Key}, skipping ...");
                }
                else
                {
                    await app.Error.WriteLineAsync($"Failed a request for {id} at {baseurl}/{id}, skipping ...");
                }

                await Task.Delay(TimeSpan.FromSeconds(0.25));
            }

            token.ThrowIfCancellationRequested();

            return coords;
        }

        private static async Task SaveResultsAsync(CancellationToken token, string output, ConcurrentDictionary<string, LatLng> coords, ConcurrentDictionary<string, int> ammountCounts,
            List<string> order, CommandLineApplication app)
        {
            token.ThrowIfCancellationRequested();
            List<string> data = new()
            { 
                "brewery, latitude, longitude" 
            };

            foreach(var b in order)
            {
                token.ThrowIfCancellationRequested();

                if(ammountCounts.TryGetValue(b, out var amnt))
                {
                    if (coords.TryGetValue(b, out var c))
                        for (int i = 0; i < amnt; i++)
                            data.Add(string.Join(", ", b, c.Latitude, c.Longitude));
                    else for (int i = 0; i < amnt; i++)
                            data.Add(string.Join(", ", b, "0", "0"));

                    Console.WriteLine($"Stored {amnt} CSV string(s) for {b}");
                }
                else
                {
                    await app.Error.WriteLineAsync($"Failed to save data for {b}, no ammount counter found.");
                }
            }

            await WriteResultsAsync(token, data, output);
            Console.WriteLine($"Results File Saved to {output}");
        }

        private static async Task WriteResultsAsync(CancellationToken token, List<string> csvdata, string output)
            => await File.WriteAllLinesAsync(output, csvdata, token);
    }
}
