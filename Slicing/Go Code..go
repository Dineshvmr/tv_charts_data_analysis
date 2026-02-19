package service

import (
	"fmt"
	"math"

	gotutilsdto "github.com/riskilla/platform/go-utils/dto"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx/types"
	log "github.com/sirupsen/logrus"

	gcommon "github.com/riskilla/platform/go-utils/common"

	"github.com/riskilla/platform/portalapi/appsetup"
	"github.com/riskilla/platform/portalapi/db"
	"github.com/riskilla/platform/portalapi/dto"
	"github.com/riskilla/platform/portalapi/helpers"
	"github.com/riskilla/platform/portalapi/utils"
)

const SLICECOUNTTHRESHOLD = 10

func TransformUserInputToInitialState(requestData []dto.UserBasketOrderData, instrumentDataMap map[string]dto.InstrumentData, freezeLimitOfUnderlyingInstrument int64, userID int64, brokerID gcommon.BrokerIDType, feature, device *string) (db.BasketOrder, []db.BasketOrderEntry, map[string]int16, bool, *dto.AppError) {
	basketID := uuid.New()
	brokerName, gErr := gcommon.BrokerMap.BasketOrderName(brokerID)
	if gErr != nil {
		return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(gErr)
	}
	initRetryStatus := utils.ToSQLString(db.RETRY_STATUS_DISABLED)
	if brokerID == gcommon.ZERODHA_BROKER_ID {
		initRetryStatus = utils.ToSQLString(db.RETRY_STATUS_ALLOWED)
	}

	basketOrder := db.BasketOrder{
		BasketOrderID: basketID,
		UserID:        userID,
		OrderTag:      utils.NewOrderEntryTag(),
		Broker:        brokerName,
		Feature:       feature,
		Device:        device,
	}
	basketEntries := []db.BasketOrderEntry{}
	emptyJSONArray := []byte("[]")
	emptyArray := types.JSONText{}
	emptyArray.Scan(emptyJSONArray)

	highestQuantity := int32(0)
	highestSliceCount := int16(0)

	// primary here refers to the trading symbol which requires the highest number of slice allocations
	var primaryTradingSymbol string
	for _, order := range requestData {

		symbolData, ok := instrumentDataMap[order.Tradingsymbol]
		if !ok {
			return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting instrument data for symbols"))
		}

		// We are trying to find the achievable freeze limit for a symbol.
		// Eg: consider NSE freeze limit of 500
		// For symbol with lot size as 50, freeze limit is 500
		// For symbol with lot size as 60, freeze limit is 480, because, it is the maximum attainable quantity for that symbol
		// We take Floor as we want the quantity to be lower than or equal to NSE freeze limit
		freezeLimitForSymbol := int64(math.Floor(float64(freezeLimitOfUnderlyingInstrument)/float64(symbolData.LotSize))) * symbolData.LotSize
		sliceCountRequired := int16(math.Ceil(float64(order.Quantity) / float64(freezeLimitForSymbol)))
		if (sliceCountRequired > highestSliceCount) || (sliceCountRequired == highestSliceCount && order.Quantity > highestQuantity) {
			highestSliceCount = sliceCountRequired
			highestQuantity = order.Quantity
			primaryTradingSymbol = order.Tradingsymbol
		}

		// FREEZE LIMT = 50
		// BNF EX1= 150, (50) -> 3 SLICE
		// BNF EX2 = 120 (30) ->  4 SLICE  ( PRIMARY SYMBOL)

		// If the slice counts tally for two instruments, we should consider the one with the higher quantity as primary symbol.
		// Only if they tally should we consider the quantity.
		// Consider two symbols A and B with quantity 150 and 120 respectively with lot size 50 and 30
		// Assume both have freeze limit of 50.
		// For A we need 3 slices and B needs 4 slices,
		// Here, B should be chosen as the primary symbol even though it's quantity is lower than that of A.
		// Therefore, the quantity check should ONLY be considered as a secondary check in cases of SLICE COUNTS' TALLYING.
		// So a single logic statement "sliceCountRequired >= highestSliceCount && order.Quantity > highestQuantity"
		// cannot be used as there can also be scenarios where slice count is higher but quantity is lower.
		if sliceCountRequired == highestSliceCount && order.Quantity > highestQuantity {
			highestSliceCount = sliceCountRequired
			highestQuantity = order.Quantity
			primaryTradingSymbol = order.Tradingsymbol
		}
	}

	var isOrderSliced = false
	// freezeLimitOfUnderlyingInstrument will be int max if we are unable to find the freeze quantity of the underlying instrument.
	// If it is int max, then the order slicing will be skipped and we go back to the default baskte page without slicing.

	// "0" is the default slice count for non-sliced basket orders
	basketOrder.SliceCount = 0

	// If we need more than "1" slice (which is basically the default) then essentially the orders will have to be sliced.
	if highestSliceCount > 1 {
		isOrderSliced = true
		basketOrder.SliceCount = highestSliceCount
		primarySymbolInstrumentData, ok := instrumentDataMap[primaryTradingSymbol]
		if !ok {
			return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting instrument data for symbols"))
		}

		//  todo-> check with sreerag if this is just a check or not.
		if highestSliceCount > SLICECOUNTTHRESHOLD {
			return db.BasketOrder{}, nil, nil, false, dto.ValidationError(
				fmt.Sprintf(
					"The maximum allowed quantity per leg is %d (%d lots). Please reduce the order quantity and try again.",
					int(freezeLimitOfUnderlyingInstrument*SLICECOUNTTHRESHOLD),
					int((freezeLimitOfUnderlyingInstrument*SLICECOUNTTHRESHOLD)/primarySymbolInstrumentData.LotSize),
				),
				nil,
			)
		}

	}

	orderEntryTagToSliceNumberMapping := make(map[string]int16)
	// If orders need not be sliced, go back to the non-sliced basket page logic
	if !isOrderSliced {

		for _, order := range requestData {
			orderInstrumentData, ok := instrumentDataMap[order.Tradingsymbol]
			if !ok {
				return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting instrument data for symbols"))
			}
			// If market order for the trade is not supported, override the the trade's order type to 'LIMIT'
			marketOrderDisabled, _ := helpers.IsMarketOrderDisabled(orderInstrumentData, brokerID)
			//  todo-> this might be able to move to the validation step
			if marketOrderDisabled && order.OrderType == "MARKET" {
				order.OrderType = "LIMIT"
			}

			entry := db.BasketOrderEntry{
				BasketOrderID:   basketID,
				Tradingsymbol:   order.Tradingsymbol,
				Product:         utils.ToSQLString(order.Product),
				Validity:        utils.ToSQLString(order.Validity),
				OrderType:       utils.ToSQLString(order.OrderType),
				Exchange:        order.Exchange,
				TransactionType: order.TransactionType,
				Quantity:        order.Quantity,
				Variety:         "regular",
				OrderState:      db.StateOrderInit,
				UserUpdates:     emptyArray,
				BrokerUpdates:   emptyArray,
				Price:           utils.ToSQLFloat64(order.Price),
				TriggerPrice:    utils.ToSQLFloat64(order.TriggerPrice),
				OrderEntryTag:   utils.NewOrderEntryTag(),
				RetryStatus:     initRetryStatus,
				//OrderGeneratedBy: "sensibull",
			}
			basketEntries = append(basketEntries, entry)
		}
	} else {
		// We maintian this so as to have easy access to order data based on tradingsymbol (we iterate through the same orders for the size of slices)
		tradingSymbolToOrderMapping := make(map[string]dto.UserBasketOrderData)
		// This is done so as to not affect the original trades(we also need these at the end to do sanity checks)
		tradingSymbolToRemainingQuantityMapping := make(map[string]int32)

		// Primary symbol is considered first irrespective of the iteration, we maitian this list to easily itreate through the remaining symbols
		var uniqueTradingSymbolsExceptPrimarySymbol []string
		for _, order := range requestData {
			tradingSymbolToOrderMapping[order.Tradingsymbol] = order
			tradingSymbolToRemainingQuantityMapping[order.Tradingsymbol] = order.Quantity
			if order.Tradingsymbol != primaryTradingSymbol {
				uniqueTradingSymbolsExceptPrimarySymbol = append(uniqueTradingSymbolsExceptPrimarySymbol, order.Tradingsymbol)
			}
		}

		primarySymbolInstrumentData, ok := instrumentDataMap[primaryTradingSymbol]
		if !ok {
			return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting instrument data for symbols"))
		}
		primaryOrder, ok := tradingSymbolToOrderMapping[primaryTradingSymbol]
		if !ok {
			return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting order data"))
		}

		for i := int16(0); i < highestSliceCount; i++ {

			primarySymbolfilledQuantity := int32(0)
			primarySymbolRemainingQuantity, ok := tradingSymbolToRemainingQuantityMapping[primaryTradingSymbol]
			if !ok {
				return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting order data"))
			}

			// Ideally this case should never occur, adding this as a sanity check anyway.
			if primarySymbolRemainingQuantity <= 0 {
				fmt.Printf("Error in slicing order : primary symbol quantity incorrect for symbol %s with quantity %d", primaryTradingSymbol, primarySymbolRemainingQuantity)
				continue
			}

			primarySymbolfilledQuantity = utils.GetFilledQuantityForPrimarySymbol(
				primarySymbolRemainingQuantity,
				freezeLimitOfUnderlyingInstrument,
				primarySymbolInstrumentData.LotSize,
			)

			// We are skipping the ok check here since it was already checked ahead in the start of this iteration
			tradingSymbolToRemainingQuantityMapping[primaryTradingSymbol] -= primarySymbolfilledQuantity
			//  todo-> this might be able to move to the validation step

			marketOrderDisabled, _ := helpers.IsMarketOrderDisabled(primarySymbolInstrumentData, brokerID)
			if marketOrderDisabled && primaryOrder.OrderType == "MARKET" {
				primaryOrder.OrderType = "LIMIT"
			}

			primarySymbolEntry := db.BasketOrderEntry{
				BasketOrderID:   basketID,
				Tradingsymbol:   primaryOrder.Tradingsymbol,
				Product:         utils.ToSQLString(primaryOrder.Product),
				Validity:        utils.ToSQLString(primaryOrder.Validity),
				OrderType:       utils.ToSQLString(primaryOrder.OrderType),
				Exchange:        primaryOrder.Exchange,
				TransactionType: primaryOrder.TransactionType,
				Quantity:        primarySymbolfilledQuantity,
				Variety:         "regular",
				OrderState:      db.StateOrderInit,
				UserUpdates:     emptyArray,
				BrokerUpdates:   emptyArray,
				Price:           utils.ToSQLFloat64(primaryOrder.Price),
				OrderEntryTag:   utils.NewOrderEntryTag(),
				RetryStatus:     initRetryStatus,
				//OrderGeneratedBy: "sensibull",
			}

			// We use orderEntryTag as unique identifier for a basket entry as at this point we don't have a db "id".
			orderEntryTagToSliceNumberMapping[primarySymbolEntry.OrderEntryTag] = i

			basketEntries = append(basketEntries, primarySymbolEntry)

			fmt.Println(primarySymbolEntry.Tradingsymbol)
			for _, tradingSymbol := range uniqueTradingSymbolsExceptPrimarySymbol {
				currentOrder, ok := tradingSymbolToOrderMapping[tradingSymbol]
				if !ok {
					return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting order data"))
				}
				symbolRemainingQuantity, ok := tradingSymbolToRemainingQuantityMapping[tradingSymbol]
				if !ok {
					return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting order quantity"))
				}

				if symbolRemainingQuantity == 0 {
					continue
				}

				filledQuantity := utils.GetFilledQuantityForTrade(
					currentOrder.Quantity,
					symbolRemainingQuantity,
					highestQuantity,
					primarySymbolfilledQuantity,
					freezeLimitOfUnderlyingInstrument,
					instrumentDataMap[tradingSymbol].LotSize,
				)

				fmt.Printf("%d/%d/%d/%d/%d/%d/%d\n", currentOrder.Quantity, symbolRemainingQuantity, highestQuantity, primarySymbolfilledQuantity, freezeLimitOfUnderlyingInstrument, instrumentDataMap[tradingSymbol].LotSize, filledQuantity)

				// We are skipping the ok check here since it was already checked ahead in the start of this iteration
				tradingSymbolToRemainingQuantityMapping[tradingSymbol] -= filledQuantity

				currentInstrumentData, ok := instrumentDataMap[currentOrder.Tradingsymbol]
				if !ok {
					return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting order instrument data"))
				}
				marketOrderDisabled, _ := helpers.IsMarketOrderDisabled(currentInstrumentData, brokerID)
				if marketOrderDisabled && currentOrder.OrderType == "MARKET" {
					currentOrder.OrderType = "LIMIT"
				}
				entry := db.BasketOrderEntry{
					BasketOrderID:   basketID,
					Tradingsymbol:   currentOrder.Tradingsymbol,
					Product:         utils.ToSQLString(currentOrder.Product),
					Validity:        utils.ToSQLString(currentOrder.Validity),
					OrderType:       utils.ToSQLString(currentOrder.OrderType),
					Exchange:        currentOrder.Exchange,
					TransactionType: currentOrder.TransactionType,
					Quantity:        filledQuantity,
					Variety:         "regular",
					OrderState:      db.StateOrderInit,
					UserUpdates:     emptyArray,
					BrokerUpdates:   emptyArray,
					Price:           utils.ToSQLFloat64(currentOrder.Price),
					OrderEntryTag:   utils.NewOrderEntryTag(),
					RetryStatus:     initRetryStatus,
					//OrderGeneratedBy: "sensibull",
				}
				// We use orderEntryTag as unique identifier for a basket entry as at this point we don't have a db "id".
				orderEntryTagToSliceNumberMapping[entry.OrderEntryTag] = i
				basketEntries = append(basketEntries, entry)

			}
		}

		// Sanity checks being done are:
		// Ensure all the required quantites have been sliced accordingly for all tradingsymbols
		// Sum of the quantities of sliced on a per symbol basis matched the original quantity
		// All quantities in a slice are a multiple of their respective lot size
		var unfilledTradingSymbol []string

		// Check for unfilled quantities
		for tradingSymbol, remainingQuantity := range tradingSymbolToRemainingQuantityMapping {
			if remainingQuantity != 0 {
				unfilledTradingSymbol = append(unfilledTradingSymbol, tradingSymbol)
			}
		}

		if len(unfilledTradingSymbol) > 0 {
			return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in slicing order : unfilled quantity remains after slicing"))
		}

		var wrongLotSizeSymbols []string
		var wrongQuantitySymbols []string
		var perSymbolTotalQuantity = make(map[string]int32)
		for _, basketEntryData := range basketEntries {
			currentSymbolInstrumentData, ok := instrumentDataMap[basketEntryData.Tradingsymbol]
			if !ok {
				return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting instrument data for symbol"))
			}

			// Checking whether the sliced quantities are multiples of lot size of the symbol
			if (basketEntryData.Quantity % int32(currentSymbolInstrumentData.LotSize)) != 0 {
				wrongLotSizeSymbols = append(wrongLotSizeSymbols, basketEntryData.Tradingsymbol)
			}
			perSymbolQuantity, ok := perSymbolTotalQuantity[basketEntryData.Tradingsymbol]
			if ok {
				perSymbolTotalQuantity[basketEntryData.Tradingsymbol] = perSymbolQuantity + basketEntryData.Quantity
			} else {
				perSymbolTotalQuantity[basketEntryData.Tradingsymbol] = basketEntryData.Quantity
			}
		}

		// We check whether there is an exact match on the per symbol sliced allocation to what the user has requested
		for symbol, totalQuantity := range perSymbolTotalQuantity {
			currentSymbolRequestedOrder, ok := tradingSymbolToOrderMapping[symbol]
			if !ok {
				return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in getting current order"))
			}
			if currentSymbolRequestedOrder.Quantity != totalQuantity {
				wrongQuantitySymbols = append(wrongQuantitySymbols, symbol)
			}
		}

		if len(wrongLotSizeSymbols) > 0 {
			return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in slicing order : wrong lot size for symbols"))
		}

		if len(wrongQuantitySymbols) > 0 {
			return db.BasketOrder{}, nil, nil, false, dto.InternalServerError(fmt.Errorf("Error in slicing order : mismatch in quantity before and after slicing"))
		}

	}
	return basketOrder, basketEntries, orderEntryTagToSliceNumberMapping, isOrderSliced, nil
}

// OrderService handles the orders states
type OrderService struct {
	AppState        *appsetup.AppState
	InitialData     []dto.UserBasketOrderData
	UserOrderData   *dto.UserBasketOrderData
	BrokerOrderData *dto.BrokerBasketOrderData
	BrokerAccess    *gotutilsdto.BrokerAccess
	Feature         *string
	Device          *string
}

// InitiateBasketOrder save the basket orders to database and returns the basket order id
func (orderSvc *OrderService) InitiateBasketOrder(userID int64, brokerID, dataFeedBrokerID gcommon.BrokerIDType, parentAccessToken string) (uuid.UUID, string, bool, *dto.AppError) {
	for _, order := range orderSvc.InitialData {
		if err := ValidateOrder(orderSvc.AppState, &order, false, dataFeedBrokerID); err != nil {
			return uuid.UUID{}, "", false, err
		}
	}
	// basketError := validateBasket(orderSvc.AppState, orderSvc.InitialData)
	// if basketError != nil {
	// 	return uuid.UUID{}, "", basketError
	// }

	// Takes null as valid value for mobile app support, caus'e uses v1 apis and this function is shared by both v1 and v2,
	// If not null this should be validated for non-empty string for web which uses v2
	// Same conditions goes for both device and feature fields
	// TODO: Remove null as valid value after mobile app uses the new api aswell
	if (orderSvc.Device != nil && *orderSvc.Device == "") || orderSvc.Feature != nil && *orderSvc.Feature == "" {
		return uuid.UUID{}, "", false, dto.ValidationError("Invalid device", nil)
	}
	if orderSvc.Feature != nil && *orderSvc.Feature == "" {
		return uuid.UUID{}, "", false, dto.ValidationError("Invalid feature", nil)
	}

	tradingsymbols := make([]string, 0, len(orderSvc.InitialData))

	for _, order := range orderSvc.InitialData {
		tradingsymbols = append(tradingsymbols, order.Tradingsymbol)
	}

	instrumentList, err := db.InstrumentsSelectBySymbols(orderSvc.AppState, tradingsymbols, dataFeedBrokerID)
	if err != nil {
		orderSvc.AppState.Log.WithFields(log.Fields{
			"err":             err,
			"instrument_list": tradingsymbols,
		}).Error("error retrieving instrument details data")
		return uuid.UUID{}, "", false, dto.InternalServerError(err)
	}

	instrumentMap := map[string]dto.InstrumentData{}
	for _, instrumentData := range instrumentList {
		instrumentMap[instrumentData.Tradingsymbol] = instrumentData
	}

	// For getting the freeze limit of the underlying instrument,
	// First try to get the data from Redis
	// If data could not be found from Redis, try from Database
	// If both fails just set the MAX VALUE as the limit
	var freezeLimitOfUnderlyingInstrument int64 = math.MaxInt64

	underlyingInstruments := []string{instrumentList[0].UnderlyingInstrument}
	redisFreezeData, redisReadErr := db.GetNSEFreezeQuantityForUnderlyingSymbolFromRedis(orderSvc.AppState, underlyingInstruments)
	if redisReadErr == nil {
		freezeLimitOfUnderlyingInstrumentFromRedis, isFreezeLimitInRedis := redisFreezeData[instrumentList[0].UnderlyingInstrument]
		if !isFreezeLimitInRedis {
			orderSvc.AppState.Log.WithFields(log.Fields{"err": fmt.Errorf("error getting freeze quantity from redis")}).Error("error getting freeze quantity from redis")
			// return uuid.UUID{}, "", dto.InternalServerError(fmt.Errorf("error getting freeze quantity from redis"))
			dbFreezedata, dberr := db.GetNSEFreezeQuantityForUnderlyingSymbols(orderSvc.AppState, underlyingInstruments)
			if dberr == nil {
				freezeLimitOfUnderlyingInstrumentFromdb, isFreezeLimitIndb := dbFreezedata[instrumentList[0].UnderlyingInstrument]
				if isFreezeLimitIndb {
					freezeLimitOfUnderlyingInstrument = freezeLimitOfUnderlyingInstrumentFromdb
				}
			}
		} else {
			freezeLimitOfUnderlyingInstrument = freezeLimitOfUnderlyingInstrumentFromRedis
		}
	} else {
		dbFreezedata, dberr := db.GetNSEFreezeQuantityForUnderlyingSymbols(orderSvc.AppState, underlyingInstruments)
		if dberr == nil {
			freezeLimitOfUnderlyingInstrumentFromdb, isFreezeLimitIndb := dbFreezedata[instrumentList[0].UnderlyingInstrument]
			if isFreezeLimitIndb {
				freezeLimitOfUnderlyingInstrument = freezeLimitOfUnderlyingInstrumentFromdb
			}
		}
	}

	basketOrder, basketEntries, orderEntryTagToSliceNumberMapping, isOrderSliced, basketInitiateFailure := TransformUserInputToInitialState(orderSvc.InitialData, instrumentMap, freezeLimitOfUnderlyingInstrument, userID, brokerID, orderSvc.Feature, orderSvc.Device)
	if basketInitiateFailure != nil {
		basketInitiateFailure.LogError(orderSvc.AppState.Log)
		orderSvc.AppState.Log.WithFields(log.Fields{"basketInitiateFailure": basketInitiateFailure}).Error("error transforming user input to inital state")
		return uuid.UUID{}, "", false, basketInitiateFailure
	}

	accessToken := utils.NewAccessToken()
	err = db.BasketOrderInitialize(orderSvc.AppState, userID, basketOrder, basketEntries, orderEntryTagToSliceNumberMapping, isOrderSliced, accessToken, parentAccessToken)
	if err != nil {
		orderSvc.AppState.Log.WithFields(log.Fields{"err": err}).Error("error saving to database")
		return uuid.UUID{}, "", false, dto.InternalServerError(err)
	}

	basketOrderRedirectToken := utils.NewAccessToken()
	err = db.SaveRedirectTokenAndAccessTokenMappingToRedis(orderSvc.AppState, basketOrderRedirectToken, accessToken)
	if err != nil {
		orderSvc.AppState.Log.WithFields(log.Fields{"err": err}).Error("error generating redirect token")
		return uuid.UUID{}, "", false, dto.InternalServerError(err)
	}

	return basketOrder.BasketOrderID, basketOrderRedirectToken, isOrderSliced, nil
}
