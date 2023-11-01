from fastapi import FastAPI
import psycopg2
import pandas as pd
from fastapi.responses import HTMLResponse
import matplotlib.pyplot as plt
from io import BytesIO
import base64

app = FastAPI()


def calc_data_plot(data):
    df = pd.DataFrame(data, columns=['Date', 'High', 'Low', 'Close', 'Volume', 'Percent Change'])

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    fig.tight_layout(pad=5.0)

    # Plot for Date vs Close
    axes[0, 0].plot(df['Date'], df['Close'])
    axes[0, 0].set_xlabel('Date')
    axes[0, 0].set_ylabel('Close')
    axes[0, 0].set_title('Stock Close Prices Over Time')

    # Plot for Date vs Volume
    axes[0,1].plot(df['Date'], df['Volume'], color='g')
    axes[0,1].set_xlabel('Date')
    axes[0,1].set_ylabel('Volume')
    axes[0,1].set_title('Stock Volume Over Time')

    # Lowest Close Price based on Year
    df['Year'] = pd.to_datetime(df['Date']).dt.year
    Lowest_close_price = df.groupby('Year')['Close'].min().reset_index()

    axes[1, 0].bar(Lowest_close_price['Year'], Lowest_close_price['Close'], color='y')
    axes[1, 0].set_xlabel('Year')
    axes[1, 0].set_ylabel('Lowest Close Price')
    axes[1, 0].set_title('Lowest Close Price Based on Year')

    # Highest Close Price based on Year
    highest_close_price = df.groupby('Year')['Close'].max().reset_index()

    axes[1, 1].bar(highest_close_price['Year'], highest_close_price['Close'], color='g')
    axes[1, 1].set_xlabel('Year')
    axes[1, 1].set_ylabel('Highest Close Price')
    axes[1, 1].set_title('Highest Close Price Based on Year')



    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    plot_data = base64.b64encode(buffer.getvalue()).decode()
    return plot_data



try:
    conn = psycopg2.connect(
        host = 'localhost',
        dbname = 'stock_data',
        user = 'bibek',
        password = 'bibek'
    )
    conn.autocommit=True
except Exception as e:
    print(f"cannot connect to database: {e} ")

@app.get("/data")
def get_data():
    cur = conn.cursor()
    query = '''select * from nabil_bank'''
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    # Create a list of dictionaries
    result = []
    for row in data:
        result.append({
            'Date': row[0],
            'High': row[1],
            'Low': row[2],
            'Close': row[3],
            'Volume': row[4],
            'Percent Change': row[5]
        })
    return result

@app.get("/")
def read_root():
    return HTMLResponse(content="<h1>Go to /data to see the data!</h1><h1>Go to /plot to see the plot!</h1>")

@app.get("/plot")
async def plot_data():
    cur = conn.cursor()
    query = '''select * from nabil_bank'''
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    plot_data = calc_data_plot(data)
    html_content = f"<div style='text-align:center;'><img src='data:image/png;base64,{plot_data}'/></div>"

    return HTMLResponse(content=html_content)









