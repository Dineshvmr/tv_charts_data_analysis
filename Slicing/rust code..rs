use crate::api_helpers::basket_order_v2_tag;
use chrono::NaiveDate;
use db::InstrumentModel;
use dto::nse_freeze_limit::NseFreezeLimitMap;
use dto::order::{OrderVariety, Product, RetryStatus, SensibullOrderState};

use ahash::HashMap;
use ahash::HashMapExt;
use order_service_data::db::BasketOrderEntryDaily;
use order_service_dto::error::{OrderServiceErrors, UnProcessableEntityErrs};
use order_service_dto::requests::UserBasketOrderData;
use std::collections::hash_map::Entry;
use tracing::error;
use uuid::Uuid;

const SLICE_COUNT_THRESHOLD: i64 = 25;

// input struct to function calculating order quantity by slicing
#[derive(Clone, Debug)]
pub struct OrderQuantityCalcInput {
    pub tradingsymbol: String,
    pub product: Product,
    pub lot_size: i64,
    pub freeze_limit: i64,
    pub original_quantity: i64,
}
impl OrderQuantityCalcInput {
    pub fn new(
        tradingsymbol: String,
        product: Product,
        lot_size: i64,
        freeze_limit: i64,
        original_quantity: i64,
    ) -> Self {
        Self {
            tradingsymbol,
            product,
            lot_size,
            freeze_limit,
            original_quantity,
        }
    }
}

//  struct which holds the remaining quantity for each tradingsymbol and product
pub struct RemainingQuantityMap {
    pub contents: HashMap<String, HashMap<Product, i64>>,
}

impl RemainingQuantityMap {
    pub fn get_remaining_quantity_for_ts_and_product(
        &self,
        tradingsymbol: &str,
        product: &Product,
    ) -> Result<i64, OrderServiceErrors> {
        let remaining_qty = self
            .contents
            .get(tradingsymbol)
            .and_then(|e| e.get(product))
            .ok_or_else(|| {
                error!(
                    tradingsymbol = ?tradingsymbol,
                    product = ?product,
                    "failed to find remaining quantity for tradingsymbol and product"
                );
                UnProcessableEntityErrs::UnableToFindRemainingQuantityForInstrument.into()
            })?;
        Ok(*remaining_qty)
    }
    pub fn update_remaining_quantity_for_ts_and_product(
        &mut self,
        ts: String,
        product: Product,
        updated_remainging_quantity: i64,
    ) {
        if let Entry::Occupied(mut per_product_remaining_qts_for_ts) = self.contents.entry(ts) {
            let t = per_product_remaining_qts_for_ts.get_mut();
            if t.get(&product).is_some() {
                t.insert(product, updated_remainging_quantity);
            }
        }
    }
    pub fn new(ip: &[OrderQuantityCalcInput]) -> Self {
        let mut remaining_quantity_map: HashMap<String, HashMap<Product, i64>> = HashMap::new();

        for i in ip.iter() {
            remaining_quantity_map
                .entry(i.tradingsymbol.clone())
                .or_insert(HashMap::new())
                .entry(i.product)
                .or_insert(i.original_quantity);
        }
        Self {
            contents: remaining_quantity_map,
        }
    }
}

// struct which holds quantities of orders in each slice for each tradingsymbol and product
#[derive(Default, PartialEq, Eq, Debug)]
pub struct OrderQuantitiesMap {
    pub contents: HashMap<String, HashMap<Product, Vec<i64>>>,
}

impl OrderQuantitiesMap {
    pub fn insert(&mut self, ts: String, product: Product, quantity: i64) {
        self.contents
            .entry(ts)
            .or_insert(HashMap::new())
            .entry(product)
            .or_insert(Vec::new())
            .push(quantity);
    }
    pub fn get_slice_info_for_ts_and_product(
        &self,
        ts: &str,
        product: &Product,
    ) -> Option<&Vec<i64>> {
        self.contents.get(ts).and_then(|e| e.get(product))
    }
}

// function which calculates the order quantities for each slice
pub fn generate_the_order_quantities(
    ip: Vec<OrderQuantityCalcInput>,
) -> Result<OrderQuantitiesMap, OrderServiceErrors> {
    // this function assumes, all the orders are for the same underlying
    // and have same freeze limit and quantities are +ve

    let mut result = OrderQuantitiesMap::default();
    let mut picked_primary_instrument: Option<(String, Product)> = None;
    let mut freeze_limit_of_underlying_instrument = 0;
    let mut lot_size_of_primary_instrument = 0;
    let mut highest_slice_count = 0;
    let mut highest_quantity = 0;

    for OrderQuantityCalcInput {
        tradingsymbol,
        product,
        lot_size,
        freeze_limit,
        original_quantity,
    } in ip.iter()
    {
        if *lot_size <= 0 {
            return Err(UnProcessableEntityErrs::InvalidLotSize.into());
        }
        let freeze_limit_for_symbol =
            (*freeze_limit as f64 / *lot_size as f64).floor() as i64 * lot_size;

        if freeze_limit_for_symbol <= 0 {
            return Err(UnProcessableEntityErrs::InvalidFreezeLimit.into());
        }
        let slice_count_required =
            (*original_quantity as f64 / freeze_limit_for_symbol as f64).ceil() as i64;

        if (slice_count_required > highest_slice_count)
            || (slice_count_required == highest_slice_count
                && *original_quantity > highest_quantity)
        {
            highest_slice_count = slice_count_required;
            highest_quantity = *original_quantity;
            picked_primary_instrument = Some((tradingsymbol.clone(), *product));
            freeze_limit_of_underlying_instrument = *freeze_limit;
            lot_size_of_primary_instrument = *lot_size;
        }
    }

    if highest_slice_count > SLICE_COUNT_THRESHOLD {
        let ui_error_message = format!(
            "The maximum allowed slice quantity per leg is {}({} lots). Please reduce the order quantity and try again.",
            freeze_limit_of_underlying_instrument * SLICE_COUNT_THRESHOLD,
            freeze_limit_of_underlying_instrument * SLICE_COUNT_THRESHOLD
                / lot_size_of_primary_instrument
        );
        return Err(UnProcessableEntityErrs::SliceCountExceedsLimit {
            error_msg: ui_error_message,
        }
        .into());
    }

    let mut remaining_quantity_map = RemainingQuantityMap::new(&ip);

    let primary_instrument_info = picked_primary_instrument
        .and_then(|(ts, p)| ip.iter().find(|x| x.tradingsymbol == ts && x.product == p))
        .ok_or_else(|| UnProcessableEntityErrs::UnableToFindPrimaryTradingSymbol.into())?;

    for _ in 0..highest_slice_count {
        let primary_symbol_remaining_quantity = remaining_quantity_map
            .get_remaining_quantity_for_ts_and_product(
                &primary_instrument_info.tradingsymbol,
                &primary_instrument_info.product,
            )?;

        let lot_size = primary_instrument_info.lot_size;

        let primary_symbol_filled_quantity = get_fill_quantity_for_primary_symbol(
            primary_symbol_remaining_quantity,
            primary_instrument_info.freeze_limit,
            lot_size,
        );

        remaining_quantity_map.update_remaining_quantity_for_ts_and_product(
            primary_instrument_info.tradingsymbol.clone(),
            primary_instrument_info.product,
            primary_symbol_remaining_quantity - primary_symbol_filled_quantity,
        );

        result.insert(
            primary_instrument_info.tradingsymbol.clone(),
            primary_instrument_info.product,
            primary_symbol_filled_quantity,
        );

        let secondary_instruments = ip.iter().filter(|x| {
            x.tradingsymbol != primary_instrument_info.tradingsymbol
                || x.product != primary_instrument_info.product
        });

        for secondary_instrument in secondary_instruments {
            let remaining_quantity = remaining_quantity_map
                .get_remaining_quantity_for_ts_and_product(
                    &secondary_instrument.tradingsymbol,
                    &secondary_instrument.product,
                )?;

            let original_quantity = secondary_instrument.original_quantity;
            let lot_size_u = secondary_instrument.lot_size;

            if remaining_quantity == 0 {
                continue;
            }

            let filled_qty_for_secondary_instrument = get_filled_quantity_for_trade(
                original_quantity,
                remaining_quantity,
                highest_quantity,
                primary_symbol_filled_quantity,
                secondary_instrument.freeze_limit,
                lot_size_u,
            )?;

            remaining_quantity_map.update_remaining_quantity_for_ts_and_product(
                secondary_instrument.tradingsymbol.clone(),
                secondary_instrument.product,
                remaining_quantity - filled_qty_for_secondary_instrument,
            );

            result.insert(
                secondary_instrument.tradingsymbol.clone(),
                secondary_instrument.product,
                filled_qty_for_secondary_instrument,
            );
        }
    }

    // Cleanup phase: Add extra slices for any symbols with remaining >0, up to threshold
    let mut max_slices_per_symbol = highest_slice_count;
    loop {
        let mut all_remaining_zero = true;
        let mut has_remaining = false;
        for input in &ip {
            let rem = remaining_quantity_map
                .get_remaining_quantity_for_ts_and_product(&input.tradingsymbol, &input.product)
                .unwrap_or(0);
            if rem > 0 {
                all_remaining_zero = false;
                has_remaining = true;
                let current_slices = result.get_slice_info_for_ts_and_product(&input.tradingsymbol, &input.product)
                    .map(|v| v.len() as i64)
                    .unwrap_or(0);
                if current_slices >= SLICE_COUNT_THRESHOLD {
                    let ui_error_message = format!(
                        "Unable to complete order for {}: remaining {} qty exceeds slice limit of {}",
                        input.tradingsymbol, rem, SLICE_COUNT_THRESHOLD
                    );
                    return Err(UnProcessableEntityErrs::SliceCountExceedsLimit {
                        error_msg: ui_error_message,
                    }.into());
                }
            }
        }
        if all_remaining_zero {
            break;
        }
        if !has_remaining || max_slices_per_symbol >= SLICE_COUNT_THRESHOLD {
            let ui_error_message = format!(
                "Unable to complete all orders: some symbols have remaining qty after {} slices (threshold {})",
                max_slices_per_symbol, SLICE_COUNT_THRESHOLD
            );
            return Err(UnProcessableEntityErrs::SliceCountExceedsLimit {
                error_msg: ui_error_message,
            }.into());
        }

        // Add extra slice: Fill individual max for each with rem >0 (no proportionality, just cap to FL)
        for input in &ip {
            let rem = remaining_quantity_map
                .get_remaining_quantity_for_ts_and_product(&input.tradingsymbol, &input.product)
                .unwrap_or(0);
            if rem > 0 {
                let freeze_limit_for_symbol = (input.freeze_limit as f64 / input.lot_size as f64).floor() as i64 * input.lot_size;
                let filled = get_fill_quantity_for_primary_symbol(rem, input.freeze_limit, input.lot_size);
                remaining_quantity_map.update_remaining_quantity_for_ts_and_product(
                    input.tradingsymbol.clone(),
                    input.product,
                    rem - filled,
                );
                result.insert(
                    input.tradingsymbol.clone(),
                    input.product,
                    filled,
                );
            }
        }
        max_slices_per_symbol += 1;
    }
    Ok(result)
}

fn get_fill_quantity_for_primary_symbol(
    remaining_quantity: i64,
    freeze_limit_for_symbol: i64,
    lot_size: i64,
) -> i64 {
    // println!("Remaining quantity :{}", remaining_quantity);
    // println!("Freeze limit for symbol :{}", freeze_limit_for_symbol);
    let fill_quantity = if remaining_quantity > freeze_limit_for_symbol {
        freeze_limit_for_symbol
    } else {
        remaining_quantity
    };

    // lot size quantity with error return happens out side the loop,
    // but adding an extra check here to avoid divide by zero error
    if lot_size <= 0 {
        return 0;
    }
    let fill_quantity_in_lots_rounded = (fill_quantity as f64 / lot_size as f64).ceil() as i64;
    let fill_quantity_in_lots_multiple = fill_quantity_in_lots_rounded * lot_size;

    if fill_quantity_in_lots_multiple > freeze_limit_for_symbol {
        return fill_quantity_in_lots_multiple - lot_size;
    }

    return fill_quantity_in_lots_multiple;
}

fn get_filled_quantity_for_trade(
    total_qty: i64,
    remaining_quantity: i64,
    highest_quantity: i64,
    primary_symbol_fill_quantity: i64,
    freeze_limit_quantity_of_underlying: i64,
    lot_size: i64,
) -> Result<i64, OrderServiceErrors> {
    if highest_quantity <= 0 {
        return Err(UnProcessableEntityErrs::InvalidQuantity.into());
    }

    let filled_quantity =
        total_qty as f64 * primary_symbol_fill_quantity as f64 / highest_quantity as f64;

    if lot_size <= 0 {
        return Err(UnProcessableEntityErrs::InvalidLotSize.into());
    }
    let filled_quantity_in_lots_rounderd = (filled_quantity / lot_size as f64).ceil();

    let mut filled_quantity_in_lots_multiple =
        (filled_quantity_in_lots_rounderd * lot_size as f64) as i64;

    if filled_quantity_in_lots_multiple > freeze_limit_quantity_of_underlying {
        filled_quantity_in_lots_multiple -= lot_size;
    }

    // NEW LOGIC: If this is likely the last slice (primary_fill < FL, primary exhausted), fill full remaining to avoid underfill
    let is_last_slice = primary_symbol_fill_quantity < freeze_limit_quantity_of_underlying;
    if is_last_slice && filled_quantity_in_lots_multiple < remaining_quantity {
        // Fill the largest lot multiple <= remaining (using floor to avoid overshoot)
        let rem_lots_rounded = (remaining_quantity as f64 / lot_size as f64).floor() as i64;
        let full_rem_multiple = rem_lots_rounded * lot_size;
        if full_rem_multiple > 0 {
            // Also cap to FL if needed, though unlikely in last slice
            let capped_full_rem = std::cmp::min(full_rem_multiple, freeze_limit_quantity_of_underlying);
            return Ok(capped_full_rem);
        }
    }

    if filled_quantity_in_lots_multiple > remaining_quantity {
        let remaining_quantity_in_lots_rounded =
            (remaining_quantity as f64 / lot_size as f64).ceil() as i64;
        let remaining_quantity_as_lot_size_multiple = remaining_quantity_in_lots_rounded * lot_size;
        if remaining_quantity_as_lot_size_multiple > remaining_quantity {
            return Ok(remaining_quantity_as_lot_size_multiple - lot_size);
        }
        return Ok(remaining_quantity_as_lot_size_multiple);
    }
    return Ok(filled_quantity_in_lots_multiple);
}

pub(crate) fn get_order_quantities_input_for_trade(
    trades: &[UserBasketOrderData],
    nse_freeze_limit_map: &NseFreezeLimitMap,
    instrument_map: &HashMap<String, InstrumentModel>,
) -> Result<Vec<OrderQuantityCalcInput>, OrderServiceErrors> {
    if trades.is_empty() {
        return Ok(vec![]);
    }

    let result:Vec<_> = trades.iter().map(|t|{
        let instrument = instrument_map.get(&t.tradingsymbol).ok_or_else(||{
            error!(?t.tradingsymbol, "failed to find instrument from instrument map for tradingsymbol");
            return  UnProcessableEntityErrs::UnableToFindInstrumentForTradingSymbol.into();

        })?;
        let freeze_limit = nse_freeze_limit_map
            .get_freeze_limit_with_default_for_missing(&instrument.underlying_instrument);


        if instrument.lot_size < 1 {
            error!(?t.tradingsymbol, "lot size is less than 1");
            return Err(UnProcessableEntityErrs::InvalidLotSize.into());

        }
        if t.quantity < 1 {
            error!(?t.tradingsymbol, "quantity is less than 1");
            return Err(UnProcessableEntityErrs::InvalidQuantity.into());
        }
        Ok(OrderQuantityCalcInput::new(
            t.tradingsymbol.clone(),
            t.product,
            instrument.lot_size as i64,
            freeze_limit,
            t.quantity,
        ))
    }).collect::<Result<Vec<_>,_>>()?;
    Ok(result)
}

pub(crate) fn generate_order_entries_from_order_quantity_map(
    user_orders: &[UserBasketOrderData],
    instrument_data: &HashMap<String, InstrumentModel>,
    basket_order_id: Uuid,
    user_id: i64,
    order_slice_map: OrderQuantitiesMap,
    internal_created_date: NaiveDate,
) -> Result<Vec<BasketOrderEntryDaily>, OrderServiceErrors> {
    let mut slice_to_order_map: Vec<BasketOrderEntryDaily> = Vec::new();
    for order in user_orders.iter() {
        let instrument = instrument_data.get(&order.tradingsymbol).ok_or_else(|| {
            error!(
                tradingsymbol=?order.tradingsymbol,
                "Failed to find instrument info for :{}",
                order.tradingsymbol
            );
            UnProcessableEntityErrs::FailedToFindInstrumentModelFromInstruments.into()
        })?;

        let slice_info = order_slice_map
            .get_slice_info_for_ts_and_product(&order.tradingsymbol, &order.product)
            .ok_or_else(|| {
                error!(
                    tradingsymbol = ?order.tradingsymbol,
                    product = ?order.product,
                    "failed to find slice info for tradingsymbol and product"
                );
                UnProcessableEntityErrs::FailedToFindSliceForTradingSymbolAndProduct.into()
            })?;

        for (slice_index, quantity_in_slice) in slice_info.iter().enumerate() {
            let basket_order_entry_id = Uuid::now_v7();
            let order_tag = basket_order_v2_tag()?;

            slice_to_order_map.push(BasketOrderEntryDaily::new(
                basket_order_entry_id,
                basket_order_id,
                user_id,
                slice_index as i32,
                SensibullOrderState::Init,
                order.tradingsymbol.clone(),
                instrument.exchange.into(),
                order.transaction_type,
                OrderVariety::Regular,
                *quantity_in_slice as i32,
                order.price,
                order.trigger_price,
                order.order_type,
                order.product,
                order.validity,
                order_tag,
                None,
                RetryStatus::RetryDisabled,
                internal_created_date,
            ));
        }
    }
    Ok(slice_to_order_map)
}
