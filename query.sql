SELECT
    dv.leasecarId,
    dv.make,
    dv.model,
    dv.year,
    dv.trimLevel,
    dv.retailPrice,
    dv.fuelType,
    dv.batteryCapacity,
    dv.acceleration,
    dv.topSpeed,
    dv.seats,
    dv.luggageSpace,
    fp.duration AS lease_duration_months,
    fp.mileage AS annual_mileage_km,
    fp.pricePerMonth AS base_price_per_month_eur,
    dc.colorName,
    dc.colorPrice AS color_add_cost_eur
FROM
    dim_vehicles dv 
JOIN
    fact_price fp ON dv.leasecarId = fp.leasecarId
JOIN
    dim_color dc ON dv.leasecarId = dc.leasecarId
ORDER BY
    dv.make, 
    dv.model, 
    fp.duration, 
    fp.mileage;