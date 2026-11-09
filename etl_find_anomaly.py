from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

SOURCE_DB = "simulator_20260720"

SOURCE_CONNECTION = {
    "host": "http://clickhouse.lab.karpov.courses:8123",
    "database": SOURCE_DB,
    "user": "student",
    "password": "dpo_python_2020",
}

default_args = {
    "owner": "ilja_matveev_bsy8446",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2026, 8, 29),
}

@dag(
    dag_id="ilja_matveev_bsy8446_etl_find_anomaly",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["ilja_matveev_bsy8446", "etl_find_anomaly"],
)
def ilja_matveev_bsy8446_etl_find_anomaly():

    @task()
    def extract_metrics_stat() -> pd.DataFrame:
        query = '''
            WITH base_metrics as (
                SELECT dt, ts, 
                uniqExactIf(user_id, (feed_likes_cnt + feed_views_cnt) > 0) uniqFeedUsers,
                uniqExact(user_id) uniqMassengerUsers,
                sum(feed_likes_cnt) likes,
                sum(feed_views_cnt) views,
                sum(msg_actions_cnt) massages,
                ROUND(if(sum(feed_views_cnt) > 0, sum(feed_likes_cnt) / sum(feed_views_cnt), 0), 3) CTR

                FROM (
                SELECT
                    date_trunc('day', time) AS dt,
                    toStartOfFifteenMinutes(time) AS ts,
                    user_id,
                    COUNT(user_id) AS msg_actions_cnt,
                    0 AS feed_likes_cnt,
                    0 AS feed_views_cnt
                FROM simulator_20260720.message_actions 
                GROUP BY dt, ts, user_id

                UNION ALL 

                SELECT 
                    date_trunc('day', time) AS dt, 
                    toStartOfFifteenMinutes(time) AS ts,
                    user_id,
                    0 AS msg_actions_cnt,
                    countIf(action = 'like') AS feed_likes_cnt,
                    countIf(action = 'view') AS feed_views_cnt
                FROM simulator_20260720.feed_actions 
                GROUP BY dt, ts, user_id
                ) t1
                 GROUP BY dt, ts
                 ORDER BY dt, ts
            ),

            base_stat as (
            SELECT dt, ts, 
                uniqFeedUsers,
                MIN(uniqFeedUsers) OVER (W ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as uniqFeedUsers_hist_min,
                quantile(0.25)(uniqFeedUsers) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as uniqFeedUsers_q25,
                quantile(0.75)(uniqFeedUsers) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as uniqFeedUsers_q75,
                uniqFeedUsers_q75 - uniqFeedUsers_q25 uniqFeedUsers_IQR,
                GREATEST(uniqFeedUsers_q25 - 4*uniqFeedUsers_IQR, 0) uniqFeedUsers_Low,
                uniqFeedUsers_q75 + 4*uniqFeedUsers_IQR uniqFeedUsers_Top,

                uniqMassengerUsers,
                MIN(uniqMassengerUsers) OVER (W ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as uniqMassengerUsers_hist_min,
                quantile(0.25)(uniqMassengerUsers) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as uniqMassengerUsers_q25,
                quantile(0.75)(uniqMassengerUsers) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as uniqMassengerUsers_q75,
                uniqMassengerUsers_q75 - uniqMassengerUsers_q25 uniqMassengerUsers_IQR,
                GREATEST(uniqMassengerUsers_q25 - 4*uniqMassengerUsers_IQR, 0) uniqMassengerUsers_Low,
                uniqMassengerUsers_q75 + 4*uniqMassengerUsers_IQR uniqMassengerUsers_Top,

                likes,
                quantile(0.25)(likes) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as likes_q25,
                quantile(0.75)(likes) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as likes_q75,
                likes_q75 - likes_q25 likes_IQR,
                GREATEST(likes_q25 - 4*likes_IQR, 0) likes_Low,
                likes_q75 + 4*likes_IQR likes_Top,

                views,
                quantile(0.25)(views) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as views_q25,
                quantile(0.75)(views) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as views_q75,
                views_q75 - views_q25 views_IQR,
                GREATEST(views_q25 - 4*views_IQR, 0) views_Low,
                views_q75 + 4*views_IQR views_Top,

                massages,
                quantile(0.25)(massages) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as massages_q25,
                quantile(0.75)(massages) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as massages_q75,
                massages_q75 - massages_q25 massages_IQR,
                GREATEST(massages_q25 - 4*massages_IQR, 0) massages_Low,
                massages_q75 + 4*massages_IQR massages_Top,
                massages NOT BETWEEN massages_Low AND massages_Top massages_is_amomaly,   

                CTR,
                quantile(0.25)(CTR) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as CTR_q25,
                quantile(0.75)(CTR) OVER (W ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as CTR_q75,
                CTR_q75 - CTR_q25 CTR_IQR,
                GREATEST(CTR_q25 - 6*CTR_IQR, 0) CTR_Low,
                CTR_q75 + 6*CTR_IQR CTR_Top,
                CTR NOT BETWEEN CTR_Low AND CTR_Top CTR_is_amomaly   

            FROM 
              base_metrics
            WINDOW W AS (ORDER BY ts ASC)
            )

            SELECT dt, ts,
                   uniqFeedUsers,
                   ifNotFinite(ROUND(quantile(0.5)(uniqFeedUsers_Low) OVER (W ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING)), 0) AS uniqFeedUsers_Low_smoothed,
                   ifNotFinite(ROUND(quantile(0.5)(uniqFeedUsers_Top) OVER (W ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING)), 0) as uniqFeedUsers_Top_smoothed,
                   CAST(uniqFeedUsers NOT BETWEEN uniqFeedUsers_Low_smoothed AND uniqFeedUsers_Top_smoothed AS UInt8) as uniqFeedUsers_is_anomaly,

                   uniqMassengerUsers,
                   ifNotFinite(ROUND(quantile(0.5)(uniqMassengerUsers_Low) OVER (W ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING)), 0) AS uniqMassengerUsers_Low_smoothed,
                   ifNotFinite(ROUND(quantile(0.5)(uniqMassengerUsers_Top) OVER (W ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING)), 0) as uniqMassengerUsers_Top_smoothed,
                   CAST(uniqMassengerUsers NOT BETWEEN uniqMassengerUsers_Low_smoothed AND uniqMassengerUsers_Top_smoothed AS UInt8) AS uniqMassengerUsers_is_anomaly,

                   likes,
                   ifNotFinite(ROUND(AVG(likes_Low) OVER (W ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)), 0) as likes_Low_smoothed,
                   ifNotFinite(ROUND(AVG(likes_Top) OVER (W ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)), 0) as likes_Top_smoothed,
                   CAST(likes NOT BETWEEN likes_Low_smoothed AND likes_Top_smoothed AS UInt8) as likes_is_anomaly,

                   views,
                   ifNotFinite(ROUND(AVG(views_Low) OVER (W ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)), 0) as views_Low_smoothed,
                   ifNotFinite(ROUND(AVG(views_Top) OVER (W ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)), 0) as views_Top_smoothed,
                   CAST(views NOT BETWEEN views_Low_smoothed AND views_Top_smoothed AS UInt8) as views_is_anomaly,       

                   massages,
                   ifNotFinite(ROUND(AVG(massages_Low) OVER (W ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)), 0) as massages_Low_smoothed,
                   ifNotFinite(ROUND(AVG(massages_Top) OVER (W ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)), 0) as massages_Top_smoothed,
                   CAST(massages NOT BETWEEN massages_Low_smoothed AND massages_Top_smoothed AS UInt8) as massages_is_anomaly,     

                   CTR,
                   ifNotFinite(ROUND(AVG(CTR_Low) OVER (W ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 3), 0) as CTR_Low_smoothed,
                   ifNotFinite(ROUND(AVG(CTR_Top) OVER (W ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 3), 0) as CTR_Top_smoothed,
                   CAST(CTR NOT BETWEEN CTR_Low_smoothed AND CTR_Top_smoothed AS UInt8) as CTR_is_anomaly

            FROM   base_stat
            WHERE dt IN (today(), yesterday())  AND ts < toStartOfFifteenMinutes(now())
            WINDOW W AS (ORDER BY ts ASC)
        '''
        
        return ph.read_clickhouse(query, connection=SOURCE_CONNECTION)
    
    @task()
    def report_anomalies(metrics_stat: pd.DataFrame) -> None:
         def plot_anomaly_charts(data, metric_name):
            min_ts = data.ts.min()
            max_ts = data.ts.max()
            col_metric = metric_name
            col_low = f"{metric_name}_Low_smoothed"
            col_top = f"{metric_name}_Top_smoothed"
            col_anomaly = f"{metric_name}_is_anomaly"
            fig = plt.figure(figsize=(14, 6))

            plt.plot(data['ts'], data[col_metric], label=col_metric, color='#1f77b4', linewidth=1.5)

            plt.plot(data['ts'], data[col_top], label='Top Smoothed', color='crimson', linestyle='--', alpha=0.7)
            plt.plot(data['ts'], data[col_low], label='Low Smoothed', color='crimson', linestyle='--', alpha=0.7)

            plt.fill_between(data['ts'], data[col_low], data[col_top], color='crimson', alpha=0.04, label='Normal Range')

            anomalies = data[data[col_anomaly].astype(int) == 1]
            plt.scatter(anomalies['ts'], anomalies[col_metric], color='orange', s=50, zorder=5, label='Anomaly')
            last_row_idx = len(anomalies) - 1
            plt.scatter(anomalies['ts'].iloc[last_row_idx], anomalies[col_metric].iloc[last_row_idx], color='red', s=50, zorder=5, label='Current anomaly')

            anomaly_value = data[col_metric].iloc[len(data) - 1]

            low_value = data[col_low].iloc[len(data) - 1]
            top_value = data[col_top].iloc[len(data) - 1]

            pred_anomaly_value = data[col_metric].iloc[len(data) - 2]
            delta_pred =  round((anomaly_value / pred_anomaly_value - 1) * 100, 2)


            border_name = 'Low' if anomaly_value < low_value else 'Top'
            delta_border =  round((anomaly_value / data[f"{metric_name}_{border_name}_smoothed"].iloc[len(data) - 1] - 1) * 100, 2)

            plt.title(f'Мониторинг метрики {metric_name}\nза период с {min_ts} по {max_ts}\n обнаружено аномальное значение {anomaly_value}\nИзм. к пред. значению: {delta_pred}%\nВыход за {border_name} на {delta_border}%', fontsize=14, fontweight='bold')
            plt.xlabel('Время', fontsize=11)
            plt.ylabel('Значение', fontsize=11)
            plt.grid(True, linestyle=':', alpha=0.6)
            plt.legend(loc='upper left', frameon=True)
            plt.xticks(rotation=15)
            plt.tight_layout()

            plot_object = io.BytesIO()
            fig.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'actual_{metric_name}_plot.png'
            plt.close()
            with open(f'actual_{metric_name}_plot.png', 'wb') as f:
                f.write(plot_object.getvalue())

         metrics_stat.ts = pd.to_datetime(metrics_stat.ts) 

         metrics = ['uniqFeedUsers', 'uniqMassengerUsers', 'likes', 'views', 'massages', 'CTR']

         alerts_count = 0
         for metric in metrics:
             if metrics_stat[::-1].iloc[0][f"{metric}_is_anomaly"] > 0:
                 alerts_count+=1
                 plot_anomaly_charts(metrics_stat, metric)

         if alerts_count: 
             print("Подробную динамику метрик за последние две недели можно посмотреть здесь: https://superset.lab.karpov.courses/superset/dashboard/8995/")

    report_anomalies(extract_metrics_stat())

ilja_matveev_bsy8446_etl_find_anomaly_dag = ilja_matveev_bsy8446_etl_find_anomaly()