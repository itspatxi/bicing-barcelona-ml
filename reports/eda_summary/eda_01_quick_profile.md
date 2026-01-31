# EDA Quick Profile

- Filas (sample): 1,000,000

- Columnas: 21

- Rango time_hour: 1970-01-01 01:00:00 -> 2025-12-31 23:00:00

- Duplicados (station_id+time_hour) en sample: 0


## Nulos (top 15)

|                      |   null_count |
|:---------------------|-------------:|
| lag_24h_bikes        |        32424 |
| lag_1h_bikes         |           88 |
| relative_humidity_2m |            2 |
| pressure_msl         |            2 |
| precipitation        |            2 |
| wind_speed_10m       |            2 |
| temperature_2m       |            2 |
| mechanical_mean      |            0 |
| time_hour            |            0 |
| station_id           |            0 |
| docks_available_mean |            0 |
| bikes_available_mean |            0 |
| ebike_mean           |            0 |
| is_holiday           |            0 |
| is_weekend           |            0 |


## Figuras generadas

- reports/figures/eda_hist_bikes_available_mean.png

- reports/figures/eda_scatter_temp_vs_bikes.png

- reports/figures/eda_line_mean_bikes_by_hour.png

- reports/figures/eda_bar_mean_bikes_by_dayofweek.png

- reports/figures/eda_box_bikes_holiday_vs_not.png
