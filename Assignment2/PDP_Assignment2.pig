/* use CSV module to avoid removing quotation marks */
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

/* Load orders.csv using the module*/
ordersCSV = LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVExcelStorage() AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);

/* Only select target Holland */
targetHolland = FILTER ordersCSV BY target == 'Holland';

/* Group targets by location and target Holland */
targetHollandGrouped = GROUP targetHolland BY (location, target);

/* Count how many times Holland was target */
targetHollandCounted = FOREACH targetHollandGrouped GENERATE group, COUNT(targetHolland);

/* Sort the list on alphabetic order */
orders = ORDER targetHollandCounted BY $0 ASC;

/* Show results */
DUMP orders;