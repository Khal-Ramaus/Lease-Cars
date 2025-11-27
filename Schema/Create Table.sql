CREATE TABLE dim_vehicles (
    leasecarId VARCHAR(50) PRIMARY KEY,
    make VARCHAR(100) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INT,
    type VARCHAR(255),
    trimLevel VARCHAR(100),
    retailPrice DECIMAL(10, 2),
    fuelType VARCHAR(10),
    batteryCapacity DECIMAL(5, 2),
    `range` DECIMAL(5, 1), 
    enginePowerHP INT,
    maxTorque INT,
    acceleration DECIMAL(4, 2),
    topSpeed INT,
    length INT,
    height INT,
    weight INT,
    seats INT,
    luggageSpace INT,
    standardFeatures_list TEXT
);

CREATE TABLE fact_price (
    price_id INT AUTO_INCREMENT PRIMARY KEY,
    leasecarId VARCHAR(50) NOT NULL,
    duration INT NOT NULL, 
    mileage INT NOT NULL,
    pricePerMonth DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (leasecarId) REFERENCES dim_vehicles(leasecarId)
);


CREATE TABLE dim_color (
    color_id INT AUTO_INCREMENT PRIMARY KEY, 
    leasecarId VARCHAR(50) NOT NULL,
    colorName VARCHAR(100) NOT NULL,
    colorCode VARCHAR(10),
    colorPrice DECIMAL(8, 2) DEFAULT 0.0,
    primaryRgbCode VARCHAR(7),
    FOREIGN KEY (leasecarId) REFERENCES dim_vehicles(leasecarId)
);