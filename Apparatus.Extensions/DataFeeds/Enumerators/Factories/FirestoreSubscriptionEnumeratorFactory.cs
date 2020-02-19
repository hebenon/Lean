using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators.Factories;
using QuantConnect.Lean.Engine.Results;
using System;
using System.Collections.Generic;

namespace Apparatus.Extensions.DataFeeds.Enumerators.Factories
{
    class FirestoreSubscriptionEnumeratorFactory : ISubscriptionEnumeratorFactory, IDisposable
    {
        private readonly bool _isLiveMode;
        private readonly bool _includeAuxiliaryData;
        private readonly IResultHandler _resultHandler;
        private readonly IFactorFileProvider _factorFileProvider;
        private readonly Func<SubscriptionRequest, IEnumerable<DateTime>> _tradableDaysProvider;
        private readonly IMapFileProvider _mapFileProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="SubscriptionDataReaderSubscriptionEnumeratorFactory"/> class
        /// </summary>
        /// <param name="resultHandler">The result handler for the algorithm</param>
        /// <param name="mapFileProvider">The map file provider</param>
        /// <param name="factorFileProvider">The factor file provider</param>
        /// <param name="dataProvider">Provider used to get data when it is not present on disk</param>
        /// <param name="includeAuxiliaryData">True to check for auxiliary data, false otherwise</param>
        /// <param name="tradableDaysProvider">Function used to provide the tradable dates to be enumerator.
        /// Specify null to default to <see cref="SubscriptionRequest.TradableDays"/></param>
        public FirestoreSubscriptionEnumeratorFactory(IResultHandler resultHandler,
            IMapFileProvider mapFileProvider,
            IFactorFileProvider factorFileProvider,
            IDataProvider dataProvider,
            bool includeAuxiliaryData,
            Func<SubscriptionRequest, IEnumerable<DateTime>> tradableDaysProvider = null
            )
        {
            _resultHandler = resultHandler;
            _mapFileProvider = mapFileProvider;
            _factorFileProvider = factorFileProvider;
            _isLiveMode = false;
            _includeAuxiliaryData = includeAuxiliaryData;
            _tradableDaysProvider = tradableDaysProvider ?? (request => request.TradableDays);
        }

        /// <summary>
        /// Creates a <see cref="SubscriptionDataReader"/> to read the specified request
        /// </summary>
        /// <param name="request">The subscription request to be read</param>
        /// <param name="dataProvider">Provider used to get data when it is not present on disk</param>
        /// <returns>An enumerator reading the subscription request</returns>
        public IEnumerator<BaseData> CreateEnumerator(SubscriptionRequest request, IDataProvider dataProvider)
        {
            var mapFileResolver = request.Configuration.TickerShouldBeMapped()
                                    ? _mapFileProvider.Get(request.Security.Symbol.ID.Market)
                                    : MapFileResolver.Empty;

            var dataReader = new FirestoreSubscriptionDataReader(request.Configuration,
                request.StartTimeLocal,
                request.EndTimeLocal,
                mapFileResolver,
                _factorFileProvider,
                _tradableDaysProvider(request),
                _isLiveMode
                );
            
            dataReader.InvalidConfigurationDetected += (sender, args) => { _resultHandler.ErrorMessage(args.Message); };
            dataReader.NumericalPrecisionLimited += (sender, args) => { _resultHandler.DebugMessage(args.Message); };
            dataReader.StartDateLimited += (sender, args) => { _resultHandler.DebugMessage(args.Message); };
            dataReader.DownloadFailed += (sender, args) => { _resultHandler.ErrorMessage(args.Message, args.StackTrace); };
            dataReader.ReaderErrorDetected += (sender, args) => { _resultHandler.RuntimeError(args.Message, args.StackTrace); };

            var result = CorporateEventEnumeratorFactory.CreateEnumerators(
                dataReader,
                request.Configuration,
                _factorFileProvider,
                dataReader,
                mapFileResolver,
                _includeAuxiliaryData,
                request.StartTimeLocal);

            return result;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
        }
    }
}
