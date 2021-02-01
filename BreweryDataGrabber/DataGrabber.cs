using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
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

            ConcurrentDictionary<string, int> LocationCounts = new();
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
                    locations.Add(brewery);
                    LocationCounts[brewery]++;
                    order.Add(brewery);
                }
            }

            token.ThrowIfCancellationRequested();

            return (locations, LocationCounts, order);
        }

        private static async Task<ConcurrentDictionary<string, int>> GetBreweryIdsAsync(CancellationToken token, HashSet<string> locations,
            string apiKey, CommandLineApplication app)
        {
            token.ThrowIfCancellationRequested();

            ConcurrentDictionary<string, int> ids = new();

            var baseurl = $"locquery/{URL_BASE_ID}/{apiKey}/";

            _client.BaseAddress = new(baseurl);

            token.ThrowIfCancellationRequested();

            foreach (var l in locations)
            {
                var request = await _client.GetAsync(l, token);

                if(request.IsSuccessStatusCode)
                {
                    token.ThrowIfCancellationRequested();

                    var raw = await request.Content.ReadAsStreamAsync();

                    var reader = XmlReader.Create(raw);

                    var rawid = reader["id"];

                    token.ThrowIfCancellationRequested();

                    if (int.TryParse(rawid, out var id))
                        ids[l] = id;
                    else
                        await app.Error.WriteLineAsync($"Failed to get ID for {l}, skipping ...");
                }
                else
                {
                    await app.Error.WriteLineAsync($"Failed a request for {l} at {baseurl}/{l}, skipping ...");   
                }
            }

            token.ThrowIfCancellationRequested();

            return ids;
        }

        private static async Task<ConcurrentDictionary<string, LatLng>> GetGlobalCoordsAsync(CancellationToken token, ConcurrentDictionary<string, int> ids,
            string apiKey, CommandLineApplication app)
        {
            token.ThrowIfCancellationRequested();

            ConcurrentDictionary<string, LatLng> coords = new();

            var baseurl = $"locmap/{URL_BASE_ID}/{apiKey}/";

            _client.BaseAddress = new(baseurl);

            token.ThrowIfCancellationRequested();

            foreach (var id in ids)
            {
                token.ThrowIfCancellationRequested();

                var request = await _client.GetAsync(id.Value.ToString(), token);

                if (request.IsSuccessStatusCode)
                {
                    token.ThrowIfCancellationRequested();

                    var raw = await request.Content.ReadAsStreamAsync();

                    var reader = XmlReader.Create(raw);

                    var rawlat = reader["lat"];
                    var rawlng = reader["lng"];

                    token.ThrowIfCancellationRequested();

                    if (double.TryParse(rawlat, out var lat) && double.TryParse(rawlng, out var lng))
                        coords[id.Key] = new()
                        {
                            Latitude = lat,
                            Longitude = lng
                        };
                    else
                        await app.Error.WriteLineAsync($"Failed to get lat/lng for {id.Key}, skipping ...");
                }
                else
                {
                    await app.Error.WriteLineAsync($"Failed a request for {id} at {baseurl}/{id}, skipping ...");
                }
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

                if(coords.TryGetValue(b, out var c)
                    && ammountCounts.TryGetValue(b, out var amnt))
                {
                    for (int i = 0; i < amnt; i++)
                        data.Add(string.Join(", ", b, c.Latitude, c.Longitude));
                }
                else
                {
                    await app.Error.WriteLineAsync($"Failed to save data for {b}, not lat/lng or ammount counter found.");
                }
            }

            await WriteResultsAsync(token, data, output);
        }

        private static async Task WriteResultsAsync(CancellationToken token, List<string> csvdata, string output)
            => await File.WriteAllLinesAsync(output, csvdata, token);
    }
}
