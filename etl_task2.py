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
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 8, 23),
}

@dag(
    dag_id="ilja_matveev_bsy8446_etl_task_2",
    default_args=default_args,
    schedule_interval="0 11 * * *",
    catchup=False,
    tags=["ilja_matveev_bsy8446", "etl_task_2"],
)
def ilja_matveev_bsy8446_etl_task_2():

    @task()
    def extract_base_metrics() -> pd.DataFrame:
        query = '''
        -- Собираем агрегаты по кол-ву действий пользователей в приложении по дням
        WITH user_daily_activity AS (
            SELECT 
                date_trunc('day', time) AS dt, 
                user_id,
                COUNT(user_id) AS msg_actions_cnt,
                0 AS feed_likes_cnt,
                0 AS feed_views_cnt
            FROM simulator_20260720.message_actions 
            GROUP BY dt, user_id

            UNION ALL 

            SELECT 
                date_trunc('day', time) AS dt, 
                user_id, 
                0 AS msg_actions_cnt,
                countIf(action = 'like') AS feed_likes_cnt,
                countIf(action = 'view') AS feed_views_cnt
            FROM simulator_20260720.feed_actions 
            GROUP BY dt, user_id
        ),

        -- Под активными пользователями понимаем тех, которые совершили хотя бы одно действие в приложении.
        daily_active_users AS (
            SELECT 
                dt,
                toStartOfMonth(dt) AS month,
                user_id,
                sum(msg_actions_cnt) AS total_msg,
                sum(feed_likes_cnt) AS total_likes,
                sum(feed_views_cnt) AS total_views,
                total_msg + total_likes + total_views AS total_actions
            FROM user_daily_activity
            GROUP BY dt, month, user_id
            HAVING total_actions > 0 AND dt <= yesterday()
        ),

        -- Считаем MAU для расчета Stickiness, одной из метрик возвращаемости пользователей
        monthly_mau AS (
            SELECT 
                month,
                uniq(user_id) AS mau_val
            FROM daily_active_users
            GROUP BY month
        ),

        -- Считаем все остальные метрики на уровне дней
        daily_metrics AS (
            SELECT 
                dt,
                month,

                -- DAU и типизация активности пользователей
                uniq(user_id) AS dau,
                uniqExactIf(user_id, total_likes > 0 AND total_msg > 0) AS core_users,
                uniqExactIf(user_id, total_likes > 0 AND total_msg = 0) AS readers_cnt,
                uniqExactIf(user_id, total_likes = 0 AND total_msg > 0) AS talkers_cnt,

                -- Активность в ленте
                sum(total_views) AS total_feed_views,
                sum(total_likes) AS total_feed_likes,
                uniqExactIf(user_id, total_views > 0) AS daily_feed_readers,
                uniqExactIf(user_id, total_likes > 0) AS daily_feed_likers,

                -- Активность в мессенджере
                sum(total_msg) AS total_messages,
                uniqExactIf(user_id, total_msg > 0) AS daily_chat_authors
            FROM daily_active_users
            GROUP BY dt, month
        )

        -- Соединяем ежедневные метрики с MAU
        SELECT 
            d.dt,
            d.dau / 1000 AS dau,
            m.mau_val / 1000 AS mau,                                                    
            d.dau / m.mau_val * 100 AS stickiness,                           

            d.core_users / 1000 AS core_users,
            d.readers_cnt / 1000 AS readers_cnt,
            d.talkers_cnt / 1000 AS talkers_cnt,

            d.total_feed_views / 1000  AS views,
            d.total_feed_likes / 1000 AS likes,
            d.total_feed_views > 0 ? (d.total_feed_likes / d.total_feed_views * 100) : 0 AS CTR,
            d.daily_feed_readers > 0 ? (d.total_feed_views / d.daily_feed_readers) : 0 AS avg_views,
            d.daily_feed_readers > 0 ? (d.daily_feed_likers / d.daily_feed_readers * 100) : 0 AS active_readers_pct,

            d.total_messages / 1000 AS messages,
            d.daily_chat_authors / 1000 AS daily_chat_authors,
            d.dau > 0 ? (d.daily_chat_authors / d.dau * 100) : 0 AS msg_penetration,
            d.daily_chat_authors > 0 ? (d.total_messages / d.daily_chat_authors) : 0 AS msg_density

        FROM daily_metrics AS d
        LEFT JOIN monthly_mau AS m ON d.month = m.month
        ORDER BY d.dt;
        '''
        
        return ph.read_clickhouse(query, connection=SOURCE_CONNECTION)

    @task()
    def extract_retention_structure() -> pd.DataFrame:
        query = '''
        -- Рассчитаем структуру Retention для всего приложения, как в более ранеем задании 
        WITH user_weekly_activity AS (
            SELECT DISTINCT toMonday(time) AS w, user_id
            FROM simulator_20260720.message_actions 
            WHERE time <= yesterday()

            UNION DISTINCT 

            SELECT DISTINCT toMonday(time) AS w, user_id
            FROM simulator_20260720.feed_actions
            WHERE time <= yesterday()
        ),

        user_weeks_array AS (
            SELECT 
                user_id,
                w AS this_week,
                addWeeks(w, -1) AS previous_week,
                addWeeks(w, 1) AS next_week,
                groupArray(w) OVER (PARTITION BY user_id) AS weeks_visited
            FROM user_weekly_activity
        ),

        new_and_retained AS (
            SELECT 
                this_week,
                status,
                toInt64(COUNT(DISTINCT user_id)) AS user_cnt
            FROM (
                SELECT 
                    user_id,
                    this_week,
                    multiIf(
                        has(weeks_visited, previous_week), 'Остались',
                        NOT(has(weeks_visited, previous_week)), 'Пришли',
                        'other'
                    ) AS status
                FROM user_weeks_array
            )
            WHERE status != 'other' AND this_week <= toMonday(yesterday())
            GROUP BY this_week, status
        ),

        gone_users AS (
            SELECT 
                next_week AS this_week,
                'Ушли' AS status,
                toInt64(COUNT(DISTINCT user_id) * -1) AS user_cnt
            FROM user_weeks_array
            WHERE NOT(has(weeks_visited, next_week)) AND next_week <= toMonday(yesterday())
            GROUP BY this_week
        ),

        union_table AS (
            SELECT this_week, status, user_cnt FROM new_and_retained
            UNION ALL 
            SELECT this_week, status, user_cnt FROM gone_users
        )

        SELECT 
            toString(this_week) AS this_week,
            status,
            max(user_cnt) / 1000 AS user_cnt
        FROM union_table
        GROUP BY this_week, status
        ORDER BY this_week ASC, multiIf(status='Пришли', 0, status='Остались', 1, 2) ASC
        '''
        
        return ph.read_clickhouse(query, connection=SOURCE_CONNECTION)   
    
    @task()
    def prepare_info(base_metrics: pd.DataFrame, retention_structure: pd.DataFrame) -> None:
        base_metrics['event_date'] = pd.to_datetime(base_metrics['dt'])

        min_date = base_metrics.event_date.min()
        max_date = base_metrics.event_date.max()

        # --- Вспомогательные функции для отрисовки графиков
        def set_min_max_last_labels(ax, dig_cnt=None):    
            def set_label(ax, x_values, y_values, pos):
                label = ("{:."+str(dig_cnt)+"f}").format(y_values[pos]) if dig_cnt else "{:.0f}".format(y_values[pos]) if y_values[pos].is_integer() else "{:.3f}".format(y_values[pos])
                ax.annotate(label, (x_values[pos], y_values[pos]), xytext=(0, 5), 
                            textcoords="offset points", ha='center', va='bottom', fontsize=12)

            line = ax.lines[0]
            y_values = line.get_ydata()
            x_values = line.get_xdata()
            id_max = np.argmax(y_values)
            id_min = np.argmin(y_values)

            set_label(ax, x_values, y_values, id_max)
            set_label(ax, x_values, y_values, id_min)
            set_label(ax, x_values, y_values, -1)

        fig, axes = plt.subplots(nrows=12, ncols=1, figsize=(14, 65))
        fig.suptitle(f'Дашборд метрик активности пользователей в приложении в целом и его компонентах\nза период с {min_date.strftime("%Y-%m-%d")} по {max_date.strftime("%Y-%m-%d")}', 
                     fontsize=22, fontweight="bold", color="black", position=(0.5, 1.005))

        def draw_plot(ax, y_col, title, color, dig=1):
            sns.lineplot(data=base_metrics, x="event_date", y=y_col, ax=ax, color=color, linewidth=2)
            ax.set_ylabel(title, fontsize=12)
            ax.set_xlabel("Дата", fontsize=11)
            ax.grid(True, linestyle='--', alpha=0.5)
            set_min_max_last_labels(ax, dig_cnt=dig)

        def draw_retention_structure_plot(ax, pivot_df):    
            for col in ['Пришли', 'Остались', 'Ушли']:
                if col not in pivot_df.columns:
                    pivot_df[col] = 0

            pivot_df.index = pd.to_datetime(pivot_df.index)
            pivot_df = pivot_df.sort_index()

            ax.bar(pivot_df.index, pivot_df['Остались'], width=5, label='Остались', color='royalblue', alpha=0.85)
            ax.bar(pivot_df.index, pivot_df['Пришли'], width=5, bottom=pivot_df['Остались'], label='Пришли', color='emerald' if 'emerald' in plt.colormaps else 'mediumseagreen', alpha=0.85)
            ax.bar(pivot_df.index, pivot_df['Ушли'], width=5, label='Ушли', color='crimson', alpha=0.85)

            ax.axhline(0, color='black', linewidth=1, linestyle='-') # Линия горизонта (ноль)
            ax.set_title('Структура изменений и удержания недельной аудитории (Retention-структура)', fontsize=16, fontweight='bold', pad=15)
            ax.set_ylabel('Количество пользователей, тыс.', fontsize=12)
            ax.set_xlabel('Начало недели', fontsize=12)

            ax.set_xticks(pivot_df.index)
            ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in pivot_df.index], rotation=45, ha='right')

            ax.grid(True, which='both', linestyle='--', alpha=0.4)

            for idx, row in pivot_df.iterrows():
                positive_sum = row['Остались'] + row['Пришли']
                negative_sum = row['Ушли']

                if positive_sum > 0:
                    ax.annotate(f"{positive_sum:.1f}", (idx, positive_sum), xytext=(0, 4), 
                                textcoords="offset points", ha='center', fontsize=10, fontweight='bold')

                if negative_sum < 0:
                    ax.annotate(f"{negative_sum:.1f}", (idx, negative_sum), xytext=(0, -12), 
                                textcoords="offset points", ha='center', fontsize=10, color='darkred')

            ax.legend(loc='upper left', fontsize=11, frameon=True)    

        pivot_df = retention_structure.pivot(index='this_week', columns='status', values='user_cnt').fillna(0)    

        # --- Метрики активности пользователей в приложении в целом---
        draw_plot(axes[0], "dau", "DAU, тыс.", "forestgreen", dig=1)
        draw_plot(axes[1], "mau", "MAU календарного месяца, тыс.", "darkgreen", dig=1)
        draw_plot(axes[2], "core_users", "Ядерная аудитория (Лайк + Сообщение), тыс.", "purple", dig=1)
        draw_plot(axes[3], "stickiness", "Stickiness, %", "teal", dig=2)

        sns.lineplot(data=base_metrics, x="event_date", y="readers_cnt", ax=axes[4], color='dodgerblue', label='Читатели')
        sns.lineplot(data=base_metrics, x="event_date", y="talkers_cnt", ax=axes[4], color='orchid', label='Собеседники')
        axes[4].set_ylabel("Сегменты аудитории, тыс.", fontsize=12)
        axes[4].set_xlabel("Дата", fontsize=11)
        axes[4].grid(True, linestyle='--', alpha=0.5)
        axes[4].legend(fontsize=11)
        # Для графика с несколькими линиями кастомный аннотатор применяется к первой линии
        set_min_max_last_labels(axes[4], dig_cnt=1) 

        draw_retention_structure_plot(axes[5], pivot_df)

        # --- Метрики ленты ---
        draw_plot(axes[6], "views", "Просмотры ленты, тыс.", "royalblue", dig=1)
        draw_plot(axes[7], "likes", "Лайки, тыс.", "crimson", dig=1)
        draw_plot(axes[8], "CTR", "CTR, %", "darkorange", dig=2)
        draw_plot(axes[9], "avg_views", "Глубина просмотра (Просмотры на юзера)", "chocolate", dig=1)

        # --- Метрики меccенджера ---
        draw_plot(axes[10], "messages", "Отправленные сообщения, тыс.", "mediumpurple", dig=1)
        draw_plot(axes[11], "msg_density", "Плотность сообщений (На автора)", "darkviolet", dig=1)

        #plt.tight_layout()
        #plt.show()
 
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics_plot.png'
        plt.close()
        with open('metrics_plot.png', 'wb') as f:
            f.write(plot_object.getvalue())

    prepare_info(extract_base_metrics(), extract_retention_structure())

ilja_matveev_bsy8446_etl_task_2_dag = ilja_matveev_bsy8446_etl_task_2()