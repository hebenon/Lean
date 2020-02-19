using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Logging;
using QuantConnect.Securities.Option;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Google.Cloud.Firestore;
using QuantConnect.Configuration;
using QuantConnect.Data.Market;

namespace Apparatus.Extensions.DataFeeds.Enumerators.Factories
{
    class FirestoreSubscriptionDataReader : IEnumerator<BaseData>, ITradableDatesNotifier, IDataProviderEvents
    {
        private bool _initialized;

        private bool _endOfStream;

        /// Configuration of the data-reader:
        private readonly SubscriptionDataConfig _config;

        /// true if we can find a scale factor file for the security of the form: ..\Lean\Data\equity\market\factor_files\{SYMBOL}.csv
        private bool _hasScaleFactors;

        // Location of the datafeed - the type of this data.

        // Create a single instance to invoke all Type Methods:
        private BaseData _dataFactory;

        //Start finish times of the backtest:
        private DateTime _periodStart;
        private readonly DateTime _periodFinish;

        private readonly MapFileResolver _mapFileResolver;
        private readonly IFactorFileProvider _factorFileProvider;
        private FactorFile _factorFile;
        private MapFile _mapFile;

        private bool _pastDelistedDate;

        // true if we're in live mode, false otherwise
        private readonly bool _isLiveMode;

        private BaseData _previous;
        private readonly IEnumerator<DateTime> _tradeableDates;

        // used when emitting aux data from within while loop
        private DateTime _delistingDate;

        private CollectionReference _collectionRef;
        private IEnumerator<DocumentSnapshot> _queryEnumerator;

        /// <summary>
        /// Subscription data reader takes a subscription request, loads the type, accepts the data source and enumerate on the results.
        /// </summary>
        /// <param name="config">Subscription configuration object</param>
        /// <param name="periodStart">Start date for the data request/backtest</param>
        /// <param name="periodFinish">Finish date for the data request/backtest</param>
        /// <param name="mapFileResolver">Used for resolving the correct map files</param>
        /// <param name="factorFileProvider">Used for getting factor files</param>
        /// <param name="tradeableDates">Defines the dates for which we'll request data, in order, in the security's data time zone</param>
        /// <param name="isLiveMode">True if we're in live mode, false otherwise</param>
        public FirestoreSubscriptionDataReader(SubscriptionDataConfig config,
            DateTime periodStart,
            DateTime periodFinish,
            MapFileResolver mapFileResolver,
            IFactorFileProvider factorFileProvider,
            IEnumerable<DateTime> tradeableDates,
            bool isLiveMode)
        {
            //Save configuration of data-subscription:
            _config = config;

            //Save Start and End Dates:
            _periodStart = periodStart;
            _periodFinish = periodFinish;
            _mapFileResolver = mapFileResolver;
            _factorFileProvider = factorFileProvider;

            //Save access to securities
            _isLiveMode = isLiveMode;
            _tradeableDates = tradeableDates.GetEnumerator();
        }

        /// <summary>
        /// Initializes the <see cref="SubscriptionDataReader"/> instance
        /// </summary>
        /// <remarks>Should be called after all consumers of <see cref="NewTradableDate"/> event are set,
        /// since it will produce events.</remarks>
        public void Initialize()
        {
            if (_initialized)
            {
                return;
            }

            //Save the type of data we'll be getting from the source.
            try
            {
                _dataFactory = _config.GetBaseDataInstance();
            }
            catch (ArgumentException exception)
            {
                OnInvalidConfigurationDetected(new InvalidConfigurationDetectedEventArgs(exception.Message));
                _endOfStream = true;
                return;
            }

            _factorFile = new FactorFile(_config.Symbol.Value, new List<FactorFileRow>());
            _mapFile = new MapFile(_config.Symbol.Value, new List<MapFileRow>());

            // load up the map files for equities, options, and custom data if it supports it.
            // Only load up factor files for equities
            if (_dataFactory.RequiresMapping())
            {
                try
                {
                    var mapFile = _mapFileResolver.ResolveMapFile(_config.Symbol, _config.Type);

                    // only take the resolved map file if it has data, otherwise we'll use the empty one we defined above
                    if (mapFile.Any()) _mapFile = mapFile;

                    if (!_config.IsCustomData && _config.SecurityType != SecurityType.Option)
                    {
                        var factorFile = _factorFileProvider.Get(_config.Symbol);
                        _hasScaleFactors = factorFile != null;
                        if (_hasScaleFactors)
                        {
                            _factorFile = factorFile;

                            // if factor file has minimum date, update start period if before minimum date
                            if (!_isLiveMode && _factorFile != null && _factorFile.FactorFileMinimumDate.HasValue)
                            {
                                if (_periodStart < _factorFile.FactorFileMinimumDate.Value)
                                {
                                    _periodStart = _factorFile.FactorFileMinimumDate.Value;

                                    OnNumericalPrecisionLimited(
                                        new NumericalPrecisionLimitedEventArgs(
                                            $"Data for symbol {_config.Symbol.Value} has been limited due to numerical precision issues in the factor file. " +
                                            $"The starting date has been set to {_factorFile.FactorFileMinimumDate.Value.ToShortDateString()}."));
                                }
                            }
                        }

                        if (_periodStart < mapFile.FirstDate)
                        {
                            var originalStart = _periodStart;
                            _periodStart = mapFile.FirstDate;

                            OnStartDateLimited(
                                new StartDateLimitedEventArgs(
                                    $"The starting date for symbol {_config.Symbol.Value}," +
                                    $" {originalStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}, has been adjusted to match map file first date" +
                                    $" {mapFile.FirstDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}."));
                        }
                    }
                }
                catch (Exception err)
                {
                    Log.Error(err, "Fetching Price/Map Factors: " + _config.Symbol.ID + ": ");
                }
            }

            // Estimate delisting date.
            switch (_config.Symbol.ID.SecurityType)
            {
                case SecurityType.Future:
                    _delistingDate = _config.Symbol.ID.Date;
                    break;
                case SecurityType.Option:
                    _delistingDate = OptionSymbol.GetLastDayOfTrading(_config.Symbol);
                    break;
                default:
                    _delistingDate = _mapFile.DelistingDate;
                    break;
            }
            // adding a day so we stop at EOD
            _delistingDate = _delistingDate.AddDays(1);

            // Perform the query and wait on the result.
            QuerySymbol().Wait();

            _initialized = true;
        }

        private async Task QuerySymbol()
        {
            // Configure Firestore connection
            FirestoreDb db = FirestoreDb.Create(Config.Get("google-cloud-project"));
            String collectionPath = $"data/{_config.Symbol.SecurityType.ToLower()}/markets/{_config.Symbol.ID.Market.ToLowerInvariant()}/symbols/daily/{_config.Symbol.Value.ToLowerInvariant()}";
            _collectionRef = db.Collection(collectionPath);
            Query dateRangeQuery = _collectionRef
                .WhereGreaterThanOrEqualTo("date", _periodStart.ToUniversalTime())
                .WhereLessThanOrEqualTo("date", _periodFinish.ToUniversalTime());

            QuerySnapshot querySnapshot = await dateRangeQuery.GetSnapshotAsync();
            _queryEnumerator = querySnapshot.GetEnumerator();

        }

        /// <summary>
        /// Event fired when an invalid configuration has been detected
        /// </summary>
        public event EventHandler<InvalidConfigurationDetectedEventArgs> InvalidConfigurationDetected;

        /// <summary>
        /// Event fired when the numerical precision in the factor file has been limited
        /// </summary>
        public event EventHandler<NumericalPrecisionLimitedEventArgs> NumericalPrecisionLimited;

        /// <summary>
        /// Event fired when the start date has been limited
        /// </summary>
        public event EventHandler<StartDateLimitedEventArgs> StartDateLimited;

        /// <summary>
        /// Event fired when there was an error downloading a remote file
        /// </summary>
        public event EventHandler<DownloadFailedEventArgs> DownloadFailed;

        /// <summary>
        /// Event fired when there was an error reading the data
        /// </summary>
        public event EventHandler<ReaderErrorDetectedEventArgs> ReaderErrorDetected;

        /// <summary>
        /// Event fired when there is a new tradable date
        /// </summary>
        public event EventHandler<NewTradableDateEventArgs> NewTradableDate;

        /// <summary>
        /// Last read BaseData object from this type and source
        /// </summary>
        public BaseData Current
        {
            get;
            private set;
        }

        /// <summary>
        /// Explicit Interface Implementation for Current
        /// </summary>
        object IEnumerator.Current
        {
            get { return Current; }
        }

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            if (!_initialized)
            {
                // Late initialization so it is performed in the data feed stack
                // and not in the algorithm thread
                Initialize();
            }

            if(_queryEnumerator.MoveNext())
            {
                Current = BuildBaseDataFromDocumentSnapshot(_queryEnumerator.Current);
                return true;
            }

            return false;
        }

        private BaseData BuildBaseDataFromDocumentSnapshot(DocumentSnapshot document)
        {
            var tradebar = _dataFactory as TradeBar;
            if(tradebar != null)
            {
                var newBar = new TradeBar
                {
                    Symbol = _config.Symbol,
                    Period = _config.Increment
                };
                newBar.Open = new decimal(document.GetValue<double>("open"));
                newBar.High = new decimal(document.GetValue<double>("high"));
                newBar.Low = new decimal(document.GetValue<double>("low"));
                newBar.Close = new decimal(document.GetValue<double>("open"));
                newBar.Volume = new decimal(document.GetValue<double>("volume"));
                newBar.Time = document.GetValue<DateTime>("date").ConvertTo(_config.DataTimeZone, _config.ExchangeTimeZone);

                return newBar;
            }

            return _dataFactory.Clone();
        }

        /// <summary>
        /// Iterates the tradeable dates enumerator
        /// </summary>
        /// <param name="date">The next tradeable date</param>
        /// <returns>True if we got a new date from the enumerator, false if it's exhausted, or in live mode if we're already at today</returns>
        private bool TryGetNextDate(out DateTime date)
        {
            if (_isLiveMode && _tradeableDates.Current >= DateTime.Today)
            {
                // special behavior for live mode, don't advance past today
                date = _tradeableDates.Current;
                return false;
            }

            while (_tradeableDates.MoveNext())
            {
                date = _tradeableDates.Current;

                OnNewTradableDate(new NewTradableDateEventArgs(date, _previous, _config.Symbol));

                if (_pastDelistedDate || date > _delistingDate)
                {
                    // if we already passed our delisting date we stop
                    _pastDelistedDate = true;
                    break;
                }

                if (!_mapFile.HasData(date))
                {
                    continue;
                }

                // don't do other checks if we haven't gotten data for this date yet
                if (_previous != null && _previous.EndTime.ConvertTo(_config.ExchangeTimeZone, _config.DataTimeZone) > _tradeableDates.Current)
                {
                    continue;
                }

                // we've passed initial checks,now go get data for this date!
                return true;
            }

            // no more tradeable dates, we've exhausted the enumerator
            date = DateTime.MaxValue.Date;
            return false;
        }

        public void Reset()
        {
            throw new NotImplementedException("Reset method not implemented. Assumes loop will only be used once.");
        }

        /// <summary>
        /// Event invocator for the <see cref="InvalidConfigurationDetected"/> event
        /// </summary>
        /// <param name="e">Event arguments for the <see cref="InvalidConfigurationDetected"/> event</param>
        protected virtual void OnInvalidConfigurationDetected(InvalidConfigurationDetectedEventArgs e)
        {
            InvalidConfigurationDetected?.Invoke(this, e);
        }

        /// <summary>
        /// Event invocator for the <see cref="NumericalPrecisionLimited"/> event
        /// </summary>
        /// <param name="e">Event arguments for the <see cref="NumericalPrecisionLimited"/> event</param>
        protected virtual void OnNumericalPrecisionLimited(NumericalPrecisionLimitedEventArgs e)
        {
            NumericalPrecisionLimited?.Invoke(this, e);
        }

        /// <summary>
        /// Event invocator for the <see cref="StartDateLimited"/> event
        /// </summary>
        /// <param name="e">Event arguments for the <see cref="StartDateLimited"/> event</param>
        protected virtual void OnStartDateLimited(StartDateLimitedEventArgs e)
        {
            StartDateLimited?.Invoke(this, e);
        }

        /// <summary>
        /// Event invocator for the <see cref="DownloadFailed"/> event
        /// </summary>
        /// <param name="e">Event arguments for the <see cref="DownloadFailed"/> event</param>
        protected virtual void OnDownloadFailed(DownloadFailedEventArgs e)
        {
            DownloadFailed?.Invoke(this, e);
        }

        /// <summary>
        /// Event invocator for the <see cref="ReaderErrorDetected"/> event
        /// </summary>
        /// <param name="e">Event arguments for the <see cref="ReaderErrorDetected"/> event</param>
        protected virtual void OnReaderErrorDetected(ReaderErrorDetectedEventArgs e)
        {
            ReaderErrorDetected?.Invoke(this, e);
        }

        /// <summary>
        /// Event invocator for the <see cref="NewTradableDate"/> event
        /// </summary>
        /// <param name="e">Event arguments for the <see cref="NewTradableDate"/> event</param>
        protected virtual void OnNewTradableDate(NewTradableDateEventArgs e)
        {
            NewTradableDate?.Invoke(this, e);
        }
    }
}
