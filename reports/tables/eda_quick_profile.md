# EDA Quick Profile

- Filas (sample): 1,000,000

- Columnas: 21

- Rango time_hour: 2019-03-28 18:00:00 -> 2025-12-31 23:00:00

- Duplicados (station_id+time_hour) en sample: 0


## Nulos (top 15)

|                      |   null_count |
|:---------------------|-------------:|
| lag_24h_bikes        |        32449 |
| lag_1h_bikes         |          103 |
| bikes_available_mean |            0 |
| time_hour            |            0 |
| station_id           |            0 |
| mechanical_mean      |            0 |
| docks_available_mean |            0 |
| ebike_mean           |            0 |
| obs_count            |            0 |
| month                |            0 |
| date                 |            0 |
| hour                 |            0 |
| dayofweek            |            0 |
| is_holiday           |            0 |
| is_weekend           |            0 |


## Figuras generadas

- reports/figures/eda_hist_bikes_available_mean.png

- reports/figures/eda_scatter_temp_vs_bikes.png

- reports/figures/eda_line_mean_bikes_by_hour.png

- reports/figures/eda_bar_mean_bikes_by_dayofweek.png

- reports/figures/eda_box_bikes_holiday_vs_not.png
