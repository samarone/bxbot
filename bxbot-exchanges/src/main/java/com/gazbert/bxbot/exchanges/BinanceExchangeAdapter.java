package com.gazbert.bxbot.exchanges;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiRestClient;
import com.binance.api.client.domain.OrderSide;
import com.binance.api.client.domain.OrderStatus;
import com.binance.api.client.domain.account.Account;
import com.binance.api.client.domain.account.AssetBalance;
import com.binance.api.client.domain.account.NewOrder;
import com.binance.api.client.domain.account.Order;
import com.binance.api.client.domain.account.request.CancelOrderRequest;
import com.binance.api.client.domain.account.request.OrderRequest;
import com.binance.api.client.domain.market.OrderBook;
import com.binance.api.client.domain.market.TickerStatistics;
import com.binance.api.client.exception.BinanceApiException;
import com.gazbert.bxbot.exchange.api.AuthenticationConfig;
import com.gazbert.bxbot.exchange.api.ExchangeAdapter;
import com.gazbert.bxbot.exchange.api.ExchangeConfig;
import com.gazbert.bxbot.exchange.api.OptionalConfig;
import com.gazbert.bxbot.exchanges.trading.api.impl.BalanceInfoImpl;
import com.gazbert.bxbot.exchanges.trading.api.impl.MarketOrderBookImpl;
import com.gazbert.bxbot.exchanges.trading.api.impl.MarketOrderImpl;
import com.gazbert.bxbot.exchanges.trading.api.impl.OpenOrderImpl;
import com.gazbert.bxbot.trading.api.BalanceInfo;
import com.gazbert.bxbot.trading.api.ExchangeNetworkException;
import com.gazbert.bxbot.trading.api.MarketOrder;
import com.gazbert.bxbot.trading.api.MarketOrderBook;
import com.gazbert.bxbot.trading.api.OpenOrder;
import com.gazbert.bxbot.trading.api.OrderType;
import com.gazbert.bxbot.trading.api.TradingApiException;

public final class BinanceExchangeAdapter extends AbstractExchangeAdapter implements ExchangeAdapter {

	private static final String BINANCE_BIGDECIMAL_PATTERN = "#.########";

	private static final Logger LOG = LogManager.getLogger();

	private BinanceApiRestClient client;

	/**
	 * The key used in the MAC message.
	 */
	private String key = "";

	/**
	 * The secret used for signing MAC message.
	 */
	private String secret = "";
	
    /**
     * Exchange buy fees in % in {@link BigDecimal} format.
     */
    private BigDecimal buyFeePercentage;

    /**
     * Exchange sell fees in % in {@link BigDecimal} format.
     */
    private BigDecimal sellFeePercentage;
    
	/**
	 * Simulate mode enabled.
	 */
	private Boolean simulateMode = Boolean.TRUE;
	
	/**
	 * Name of PUBLIC key prop in config file.
	 */
	private static final String KEY_PROPERTY_NAME = "key";

	/**
	 * Name of secret prop in config file.
	 */
	private static final String SECRET_PROPERTY_NAME = "secret";
	
    /**
     * Name of buy fee property in config file.
     */
    private static final String BUY_FEE_PROPERTY_NAME = "buy-fee";

    /**
     * Name of sell fee property in config file.
     */
    private static final String SELL_FEE_PROPERTY_NAME = "sell-fee";
    
	/**
	 * Flag for Simulate Mode, default is TRUE.
	 */
	private static final String SIMULATE_MODE_PROPERTY_NAME = "simulate-mode";

	/**
	 * Used for reporting unexpected errors.
	 */
	private static final String UNEXPECTED_ERROR_MSG = "Unexpected error has occurred in Binance Exchange Adapter. ";

	/**
	 * Unexpected IO error message for logging.
	 */
	private static final String UNEXPECTED_IO_ERROR_MSG = "Failed to connect to Exchange due to unexpected IO error.";
	
	/**
	 * Hack for simulate mode
	 */
	private static int simulateOrderId = 0;

	// ------------------------------------------------------------------------------------------------
	// Config methods
	// ------------------------------------------------------------------------------------------------

	private void setAuthenticationConfig(ExchangeConfig exchangeConfig) {
		final AuthenticationConfig authenticationConfig = getAuthenticationConfig(exchangeConfig);
		key = getAuthenticationConfigItem(authenticationConfig, KEY_PROPERTY_NAME);
		secret = getAuthenticationConfigItem(authenticationConfig, SECRET_PROPERTY_NAME);
        try {
        	simulateMode = Boolean.valueOf(getAuthenticationConfigItem(authenticationConfig, SIMULATE_MODE_PROPERTY_NAME));			
		} catch (Exception e) {
			throw new RuntimeException("Erro while load "+ SIMULATE_MODE_PROPERTY_NAME, e);
		}
	}
	
    private void setOptionalConfig(ExchangeConfig exchangeConfig) {

        final OptionalConfig optionalConfig = getOptionalConfig(exchangeConfig);

        final String buyFeeInConfig = getOptionalConfigItem(optionalConfig, BUY_FEE_PROPERTY_NAME);
        buyFeePercentage = new BigDecimal(buyFeeInConfig).divide(new BigDecimal("100"), 8, BigDecimal.ROUND_HALF_UP);
        LOG.info(() -> "Buy fee % in BigDecimal format: " + buyFeePercentage);

        final String sellFeeInConfig = getOptionalConfigItem(optionalConfig, SELL_FEE_PROPERTY_NAME);
        sellFeePercentage = new BigDecimal(sellFeeInConfig).divide(new BigDecimal("100"), 8, BigDecimal.ROUND_HALF_UP);
        LOG.info(() -> "Sell fee % in BigDecimal format: " + sellFeePercentage);
    }

	// ------------------------------------------------------------------------------------------------
	// Util methods
	// -----------------------------------------------------------------------------------------------

	private com.binance.api.client.domain.OrderSide orderSideFrom(OrderType type) {
		if (OrderType.BUY.equals(type))
			return com.binance.api.client.domain.OrderSide.BUY;
		else if (OrderType.SELL.equals(type))
			return com.binance.api.client.domain.OrderSide.SELL;
		else {
			final String errorMsg = "Invalid order type: " + type + " - Can only be " + OrderType.BUY.getStringValue()
					+ " or " + OrderType.SELL.getStringValue();
			LOG.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}
	};

	private OrderType orderTypeFrom(com.binance.api.client.domain.OrderSide side) {
		if (com.binance.api.client.domain.OrderSide.BUY.equals(side))
			return OrderType.BUY;
		else if (com.binance.api.client.domain.OrderSide.SELL.equals(side))
			return OrderType.SELL;
		else {
			final String errorMsg = "Invalid order side: " + side + " - Can only be "
					+ com.binance.api.client.domain.OrderSide.BUY.name() + " or "
					+ com.binance.api.client.domain.OrderSide.SELL.name();
			LOG.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}
	};

	// ------------------------------------------------------------------------------------------------
	// Transform Functions
	// ------------------------------------------------------------------------------------------------

	private Function<com.binance.api.client.domain.market.OrderBookEntry, MarketOrder> marketOrderFrom = order -> {
		return new MarketOrderImpl(OrderType.SELL, new BigDecimal(order.getPrice()), new BigDecimal(order.getQty()),
				new BigDecimal(order.getPrice()).multiply(new BigDecimal(order.getQty())));
	};

	private Function<com.binance.api.client.domain.market.OrderBookEntry, MarketOrder> orderBookEntryFrom = order -> {
		return new MarketOrderImpl(OrderType.BUY, new BigDecimal(order.getPrice()), new BigDecimal(order.getQty()),
				new BigDecimal(order.getPrice()).multiply(new BigDecimal(order.getQty())));
	};

	private Function<com.binance.api.client.domain.account.Order, OpenOrder> transformOpenOrderFunction = (order) -> {

		final BigDecimal origQty = new BigDecimal(order.getOrigQty());
		final BigDecimal executedQty = new BigDecimal(order.getExecutedQty());
		final BigDecimal price = new BigDecimal(order.getPrice());
		final OrderType orderType = com.binance.api.client.domain.OrderSide.BUY.equals(order.getSide()) ? OrderType.BUY
				: OrderType.SELL;
		final String orderId = String.valueOf(order.getOrderId());
		final Date creationDate = new Date(order.getTime());

		return new OpenOrderImpl(orderId, creationDate, order.getSymbol(), orderType, price,
				origQty.subtract(executedQty), origQty, price.multiply(origQty.subtract(executedQty)));
	};

	private Function<com.binance.api.client.domain.account.AssetBalance, BigDecimal> balanceValueFrom = assetBalance -> {
		final BigDecimal free = new BigDecimal(assetBalance.getFree());
		final BigDecimal locked = new BigDecimal(assetBalance.getLocked());
		return free.add(locked);
	};
	
	// ------------------------------------------------------------------------------------------------
	// Main Functions
	// ------------------------------------------------------------------------------------------------

	@Override
	public void init(ExchangeConfig config) {
		LOG.info(() -> "About to initialise Binance ExchangeConfig: " + config);
		setAuthenticationConfig(config);
		setNetworkConfig(config);
		setOptionalConfig(config);
		initBinanceClient(config);
	}

	private void initBinanceClient(ExchangeConfig config) {
		BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance(key, secret);
		client = factory.newRestClient();
	}

	@Override
	public String getImplName() {
		return "Binance API v1";
	}

	@Override
	public MarketOrderBook getMarketOrders(String marketId) throws ExchangeNetworkException, TradingApiException {

		LOG.info(() -> "Find my orders from Market Id: "+ marketId.toUpperCase());
			
		try {

			OrderBook binanceOrderBook = client.getOrderBook(marketId.toUpperCase(), 100);
			
			List<MarketOrder> marketOrderSell = binanceOrderBook.getAsks().stream().map(marketOrderFrom)
					.collect(Collectors.toList());

			List<MarketOrder> marketOrderBuy = binanceOrderBook.getAsks().stream().map(orderBookEntryFrom)
					.collect(Collectors.toList());

			return new MarketOrderBookImpl(marketId, marketOrderSell, marketOrderBuy);

		} catch (BinanceApiException e) {
			throw new TradingApiException(UNEXPECTED_IO_ERROR_MSG, e);
		} catch (Exception e) {
			LOG.error(UNEXPECTED_ERROR_MSG, e);
			throw new TradingApiException(UNEXPECTED_ERROR_MSG, e);
		}
	}

	@Override
	public List<OpenOrder> getYourOpenOrders(String marketId) throws ExchangeNetworkException, TradingApiException {
	
		try {
			List<Order> openOrders = client.getOpenOrders(new OrderRequest(marketId.toUpperCase()));

			return openOrders.stream()
					.filter(t -> !OrderStatus.FILLED.equals(t.getStatus()))
					.map(transformOpenOrderFunction).collect(Collectors.toList());
				
		} catch (BinanceApiException e) {
			throw new TradingApiException(UNEXPECTED_IO_ERROR_MSG, e);
		} catch (Exception e) {
			LOG.error(UNEXPECTED_ERROR_MSG, e);
			throw new TradingApiException(UNEXPECTED_ERROR_MSG, e);
		}
	}

	@Override
	public String createOrder(String marketId, OrderType orderType, BigDecimal quantity, BigDecimal price)
			throws TradingApiException {
		
		try {
			final OrderSide side = orderSideFrom(orderType);

			// note we need to limit amount and price to 8 decimal places else exchange will barf
			final String strQuantity = new DecimalFormat(BINANCE_BIGDECIMAL_PATTERN, getDecimalFormatSymbols())
					.format(quantity);
			final String strPrice = new DecimalFormat(BINANCE_BIGDECIMAL_PATTERN, getDecimalFormatSymbols())
					.format(price);

			NewOrder newOrder = new com.binance.api.client.domain.account.NewOrder(marketId.toUpperCase(), side,
					com.binance.api.client.domain.OrderType.LIMIT, com.binance.api.client.domain.TimeInForce.GTC,
					strQuantity, strPrice);

			if (simulateMode) {
				
				LOG.info("CAll newOrderTest");
				//client.newOrderTest(newOrder);
				LOG.info("DUMMY_ORDER_ID: " + UUID.randomUUID().toString());
				return String.valueOf(simulateOrderId++);
				
			} else {
				
				//final NewOrderResponse response = client.newOrder(newOrder);
				//LOG.debug(() -> "Create Order response: " + response);
				
				LOG.info("REAL_ORDER_ID: " + UUID.randomUUID().toString());
				return String.valueOf(simulateOrderId++);
				//return response.getOrderId().toString();
			}

		} catch (BinanceApiException e) {
			throw new TradingApiException(UNEXPECTED_IO_ERROR_MSG, e);
		} catch (Exception e) {
			LOG.error(UNEXPECTED_ERROR_MSG, e);
			throw new TradingApiException(UNEXPECTED_ERROR_MSG, e);
		}
	}

	@Override
	public boolean cancelOrder(String orderId, String marketId) throws TradingApiException {
		try {
			CancelOrderRequest request = new CancelOrderRequest(marketId.toUpperCase(), orderId);
			client.cancelOrder(request);
			LOG.debug(
					() -> "Order Id: " + orderId + " from Market Id: " + marketId.toUpperCase() + " was Cancelled with successfully");
			return true;

		} catch (BinanceApiException e) {
			// TODO Handle binance errors
			final String errorMsg = "Failed to cancel order on exchange. Order Id: " + orderId;
			LOG.error(errorMsg, e);
		} catch (Exception e) {
			LOG.error(UNEXPECTED_ERROR_MSG, e);
		}

		return false;
	}

	@Override
	public BigDecimal getLatestMarketPrice(String marketId) throws ExchangeNetworkException, TradingApiException {

		try {
			TickerStatistics tickerStatistics = client.get24HrPriceStatistics(marketId.toUpperCase());
			return new BigDecimal(tickerStatistics.getLastPrice());

		} catch (BinanceApiException e) {
			throw new TradingApiException(UNEXPECTED_IO_ERROR_MSG, e);
		} catch (Exception e) {
			LOG.error(UNEXPECTED_ERROR_MSG, e);
			throw new TradingApiException(UNEXPECTED_ERROR_MSG, e);
		}

	}

	@Override
	public BalanceInfo getBalanceInfo() throws ExchangeNetworkException, TradingApiException {
		try {
			Account account = client.getAccount();
			
			List<AssetBalance> binanceBalances = account.getBalances();

			Map<String, BigDecimal> balancesAvailable = Collections.emptyMap();

			/*
			 * The adapter only fetches the 'exchange' account balance details - this is the
			 * Binance 'exchange' account, i.e. the limit order trading account balance.
			 */
			if (binanceBalances != null) {

				balancesAvailable = binanceBalances.stream()
						.collect(Collectors.toMap(AssetBalance::getAsset, balanceValueFrom));
			}

			// 2nd arg of BalanceInfo constructor for reserved/on-hold balances is not
			// provided by exchange.
			return new BalanceInfoImpl(balancesAvailable, Collections.emptyMap());

		} catch (BinanceApiException e) {
			throw new TradingApiException(UNEXPECTED_IO_ERROR_MSG, e);
		} catch (Exception e) {
			LOG.error(UNEXPECTED_ERROR_MSG, e);
			throw new TradingApiException(UNEXPECTED_ERROR_MSG, e);
		}
	}

	@Override
	public BigDecimal getPercentageOfBuyOrderTakenForExchangeFee(String marketId)
			throws TradingApiException, ExchangeNetworkException {
		return buyFeePercentage;
	}

	@Override
	public BigDecimal getPercentageOfSellOrderTakenForExchangeFee(String marketId)
			throws TradingApiException, ExchangeNetworkException {
		return sellFeePercentage;
	}
}