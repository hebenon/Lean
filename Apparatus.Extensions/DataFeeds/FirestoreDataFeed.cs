using QuantConnect;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.Results;
using QuantConnect.Logging;
using QuantConnect.Packets;
using System;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using QuantConnect.Data;
using Apparatus.Extensions.DataFeeds.Enumerators.Factories;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators.Factories;
using QuantConnect.Securities;

namespace Apparatus.Extensions.DataFeeds
{
    class FirestoreDataFeed : IDataFeed
    {
        private IAlgorithm _algorithm;
        private ITimeProvider _timeProvider;
        private IResultHandler _resultHandler;
        private IMapFileProvider _mapFileProvider;
        private IFactorFileProvider _factorFileProvider;
        private IDataProvider _dataProvider;
        private SubscriptionCollection _subscriptions;
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private FirestoreSubscriptionEnumeratorFactory _subscriptionFactory;

        public bool IsActive
        {
            get; private set;
        }

        public Subscription CreateSubscription(SubscriptionRequest request)
        {
            return request.IsUniverseSubscription
                ? CreateUniverseSubscription(request)
                : CreateDataSubscription(request);
        }

        /// <summary>
        /// Adds a new subscription for universe selection
        /// </summary>
        /// <param name="request">The subscription request</param>
        private Subscription CreateUniverseSubscription(SubscriptionRequest request)
        {
            ISubscriptionEnumeratorFactory factory = _subscriptionFactory;
            if (request.Universe is ITimeTriggeredUniverse)
            {
                factory = new TimeTriggeredUniverseSubscriptionEnumeratorFactory(request.Universe as ITimeTriggeredUniverse,
                    MarketHoursDatabase.FromDataFolder(),
                    _timeProvider);

                if (request.Universe is UserDefinedUniverse)
                {
                    // for user defined universe we do not use a worker task, since calls to AddData can happen in any moment
                    // and we have to be able to inject selection data points into the enumerator
                    return SubscriptionUtils.Create(request, factory.CreateEnumerator(request, _dataProvider));
                }
            }
            if (request.Configuration.Type == typeof(CoarseFundamental))
            {
                factory = new BaseDataCollectionSubscriptionEnumeratorFactory();
            }
            if (request.Universe is OptionChainUniverse)
            {
                factory = new OptionChainUniverseSubscriptionEnumeratorFactory((req, e) => ConfigureEnumerator(req, true, e),
                    _mapFileProvider.Get(request.Security.Symbol.ID.Market), _factorFileProvider);
            }
            if (request.Universe is FuturesChainUniverse)
            {
                factory = new FuturesChainUniverseSubscriptionEnumeratorFactory((req, e) => ConfigureEnumerator(req, true, e));
            }

            // define our data enumerator
            var enumerator = factory.CreateEnumerator(request, _dataProvider);
            return SubscriptionUtils.CreateAndScheduleWorker(request, enumerator);
        }

        private Subscription CreateDataSubscription(SubscriptionRequest request)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            if (!request.TradableDays.Any())
            {
                _algorithm.Error(
                    $"No data loaded for {request.Security.Symbol} because there were no tradeable dates for this security."
                );
                return null;
            }

            // ReSharper disable once PossibleMultipleEnumeration
            var enumerator = _subscriptionFactory.CreateEnumerator(request, _dataProvider);
            enumerator = ConfigureEnumerator(request, false, enumerator);

            return SubscriptionUtils.CreateAndScheduleWorker(request, enumerator);
        }

        public void Exit()
        {
            if (IsActive)
            {
                IsActive = false;
                Log.Trace("FileSystemDataFeed.Exit(): Start. Setting cancellation token...");
                _cancellationTokenSource.Cancel();
                //_subscriptionFactory?.DisposeSafely();
                Log.Trace("FileSystemDataFeed.Exit(): Exit Finished.");
            }
        }

        public void Initialize(IAlgorithm algorithm,
            AlgorithmNodePacket job,
            IResultHandler resultHandler,
            IMapFileProvider mapFileProvider,
            IFactorFileProvider factorFileProvider,
            IDataProvider dataProvider,
            IDataFeedSubscriptionManager subscriptionManager,
            IDataFeedTimeProvider dataFeedTimeProvider)
        {
            _algorithm = algorithm;
            _resultHandler = resultHandler;
            _mapFileProvider = mapFileProvider;
            _factorFileProvider = factorFileProvider;
            _dataProvider = dataProvider;
            _timeProvider = dataFeedTimeProvider.FrontierTimeProvider;
            _subscriptions = subscriptionManager.DataFeedSubscriptions;
            _cancellationTokenSource = new CancellationTokenSource();
            _subscriptionFactory = new FirestoreSubscriptionEnumeratorFactory(
                _resultHandler,
                _mapFileProvider,
                _factorFileProvider,
                _dataProvider,
                includeAuxiliaryData: true);

            IsActive = true;
        }

        public void RemoveSubscription(Subscription subscription)
        {
        }

        /// <summary>
        /// Configure the enumerator with aggregation/fill-forward/filter behaviors. Returns new instance if re-configured
        /// </summary>
        private IEnumerator<BaseData> ConfigureEnumerator(SubscriptionRequest request, bool aggregate, IEnumerator<BaseData> enumerator)
        {
            if (aggregate)
            {
                enumerator = new BaseDataCollectionAggregatorEnumerator(enumerator, request.Configuration.Symbol);
            }

            // optionally apply fill forward logic, but never for tick data
            if (request.Configuration.FillDataForward && request.Configuration.Resolution != Resolution.Tick)
            {
                // copy forward Bid/Ask bars for QuoteBars
                if (request.Configuration.Type == typeof(QuoteBar))
                {
                    enumerator = new QuoteBarFillForwardEnumerator(enumerator);
                }

                var fillForwardResolution = _subscriptions.UpdateAndGetFillForwardResolution(request.Configuration);

                enumerator = new FillForwardEnumerator(enumerator, request.Security.Exchange, fillForwardResolution,
                    request.Security.IsExtendedMarketHours, request.EndTimeLocal, request.Configuration.Resolution.ToTimeSpan(), request.Configuration.DataTimeZone, request.StartTimeLocal);
            }

            // optionally apply exchange/user filters
            if (request.Configuration.IsFilteredSubscription)
            {
                enumerator = SubscriptionFilterEnumerator.WrapForDataFeed(_resultHandler, enumerator, request.Security, request.EndTimeLocal);
            }

            return enumerator;
        }
    }
}
