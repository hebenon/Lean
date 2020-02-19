using System;
using System.IO;
using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.ToolBox;
using QuantConnect.ToolBox.IEX;
using QuantConnect.Util;

namespace Apparatus.Extensions.DataFeeds
{
    /// <summary>
    /// An instance of the <see cref="IDataProvider"/> that will attempt to retrieve files not present on the filesystem from IEX Cloud
    /// </summary>
    public class IEXDataProvider : IDataProvider
    {
        private readonly int _uid = Config.GetInt("job-user-id", 0);
        private readonly string _apiKey = Config.Get("iex-api-key", "1");
        private readonly string _dataPath = Config.Get("data-folder", "../../../Data/");
        private readonly IEXDataDownloader _dataDownloader;

        public IEXDataProvider()
        {
            _dataDownloader = new IEXDataDownloader(_apiKey);
        }

        public Stream Fetch(string key)
        {
            if (File.Exists(key))
            {
                return new FileStream(key, FileMode.Open, FileAccess.Read, FileShare.Read);
            }

            // If the file cannot be found on disc, attempt to retrieve it from the API
            Symbol symbol;
            DateTime date;
            Resolution resolution;

            if (LeanData.TryParsePath(key, out symbol, out date, out resolution))
            {
                Log.Trace("ApiDataProvider.Fetch(): Attempting to get data from QuantConnect.com's data library for symbol({0}), resolution({1}) and date({2}).",
                    symbol.Value,
                    resolution,
                    date.Date.ToShortDateString());

                var downloadSuccessful = DownloadSymbol(symbol, resolution, date);

                if (downloadSuccessful)
                {
                    Log.Trace("ApiDataProvider.Fetch(): Successfully retrieved data for symbol({0}), resolution({1}) and date({2}).",
                        symbol.Value,
                        resolution,
                        date.Date.ToShortDateString());

                    return new FileStream(key, FileMode.Open, FileAccess.Read, FileShare.Read);
                }
            }

            Log.Error("ApiDataProvider.Fetch(): Unable to remotely retrieve data for path {0}. " +
                      "Please make sure you have the necessary data in your online QuantConnect data library.",
                       key);

            return null;
        }

        private bool DownloadSymbol(string symbol, Resolution resolution, DateTime date)
        {
            try
            {
                const string market = Market.USA;
                SecurityType securityType = SecurityType.Equity;

                // Download the data
                var symbolObject = Symbol.Create(symbol, securityType, market);
                var data = _dataDownloader.Get(symbolObject, resolution, date, date.AddDays(1));

                // Save the data
                var writer = new LeanDataWriter(resolution, symbolObject, _dataPath);
                writer.Write(data);

                return true;
            }
            catch(Exception err)
            {
                Log.Error(err);
                return false;
            }
        }
    }
}
