from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import redshift_connector
import pandas as pd

# create connector
conn = redshift_connector.connect(
    host='redshift-db-us-west-1.aws.amazon.com',
    port=5439,
    database='my_database',
    user="root",
    password="password"
)

# create cursor object
cur = conn.cursor()

# execute query
cur.execute(f"""
    with base as (
        select date(convert_timezone('UTC','America/Los_Angeles',charge_date)) as revenue_date
            , count(charge_id) as orders
            , sum(revenue) as total_revenue
        from fact_table_schema.fact_table_revenue
        where charge_type in ('type1','type1')
        group by 1
    )
    select revenue_date
        , to_char(revenue_date, 'Day') as day_of_week
        , round(total_revenue) as revenue
        , orders as count
    from base
    where date(revenue_date) = date(convert_timezone('UTC','America/Los_Angeles',current_timestamp::timestamp - interval '1 day'));
    """
)

# turn results into dataframe
result = cur.fetchall()
columns = [desc[0] for desc in cur.description]
df = pd.DataFrame(result, columns=columns)

# create metrics
previous_day = str(df['revenue_date'][0])
day_of_week = str(df['day_of_week'][0]).capitalize()
revenue = df['revenue'][0]
revenue = str(f"{revenue:,.0f}")
count = str(df['count'][0])

# Slack Token
slack_bot_token = "token_token"

# Initialize the Slack client
client = WebClient(token=slack_bot_token)

# define expected and goals (fictional values)
expected = "1,000,000" if int(count) >= 5,000 else "500,000"
goal_1 = "550,000"
goal_2 = "1,100,000"

# Specify the slack channel and message
channel_id = "#alerts-channel-911"
message_text = f"""
<*@here*>
*{day_of_week}* *{previous_day}* *Recurring* *Revenue* *updates*:
        *Actual* *${revenue}* *vs* *Expected* *${expected}*
        *Count:* *{count}*
 *Goal:*
   *Expect* *daily* *recurring* *revenue:* *${goal_1}* *(no big retry)* *${goal_2}* *(with big retry, 3 times a month)*
"""

try:
    # Send the message
    response = client.chat_postMessage(
        channel=channel_id,
        text=message_text
    )
    print(f"Message posted successfully: {response['message']['text']}")

except SlackApiError as e:
    print(f"Error posting message: {e.response['error']}")

                                                                             
