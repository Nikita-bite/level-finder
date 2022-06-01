import math
import random
from datetime import datetime, time
import time

import requests as requests
import bokeh
from bokeh.io import curdoc
from bokeh.layouts import column, row
from math import pi
import pandas as pd
from bokeh.plotting import figure, show
import threading
#import _thread
from multiprocessing import Pool, Process, Array, Manager


def magic(p, tf, w, i):
        start = time.time()
        print(start)
        #for i in range(50):
        print(t[i]['ticker']['ticker'], tf)
        param = {'symbol': t[i]['ticker']['ticker'], 'interval': tf}
        klines_json = requests.request('GET', host + url, params=param, headers=headers)
        klines = klines_json.json()
        #print(t[i]['ticker']['ticker'], klines[len(klines) - 1][2])
        req = time.time()
        print("req: ", tf,  req - start)

        for j in range(len(klines)):
            klines[j][0] = datetime.fromtimestamp(klines[j][0] / 1000)

        df = pd.DataFrame(klines)
        df[0] = pd.to_datetime(df[0])
        df[12] = df.index

        inc = df[4] > df[1]
        dec = df[1] > df[4]  # open = 1 close = 4

        period = 10  #3
        window = 2 * period + 1
        df_pivot_high = pd.DataFrame()
        df_pivot_high["is_high"] = df[2].rolling(window, center=True).apply(lambda x: x[period] == max(x), raw=True)
        df_pivot_high["price"] = df[2][df_pivot_high["is_high"] == 1]
        df_pivot_high["time"] = df[0]
        df_pivot_high["index"] = df[12]
        df_pivot_high = df_pivot_high.dropna(subset=["price"])
        df_lines_high = pd.DataFrame()
        init = time.time()

        for j, row_1 in df_pivot_high.iterrows():
            for k, row_2 in df_pivot_high.iterrows():
                if pd.notna(df_pivot_high.at[j, 'price']) and pd.notna(df_pivot_high.at[k, 'price']):
                    if j < k and df_pivot_high.at[j, "price"] < df_pivot_high.at[k, "price"]:
                        df_pivot_high.at[j, 'price'] = pd.NA
        df_pivot_high = df_pivot_high.dropna(subset=["price"])

        delta_x = 500
        delta_y = float(df[2].max()) - float(df[3].min())
        k_1 = delta_x / delta_y
        k_2 = 2.35

        df_line_1 = pd.DataFrame()
        df_line_2 = pd.DataFrame()
        counter_lines = 0
        for x_1 in range(df_pivot_high.shape[0]):
            for x_2 in range(df_pivot_high.shape[0]):
                try:
                    if x_1 < x_2:
                        df_line_1.at[counter_lines, "x_1"] = df_pivot_high.iloc[x_1, 2]
                        df_line_1.at[counter_lines, "x_2"] = df_pivot_high.iloc[x_2, 2]
                        df_line_1.at[counter_lines, "y_1"] = float(df_pivot_high.iloc[x_1, 1])
                        df_line_1.at[counter_lines, "y_2"] = float(df_pivot_high.iloc[x_2, 1])
                        df_line_1.at[counter_lines, "index_1"] = float(df_pivot_high.iloc[x_1, 3])
                        df_line_1.at[counter_lines, "index_2"] = float(df_pivot_high.iloc[x_2, 3])
                        df_line_1.at[counter_lines, "S line"] = math.sqrt((((df_pivot_high.iloc[x_2, 3] / k_1) * k_2 - (df_pivot_high.iloc[x_1, 3] / k_1) * k_2) ** 2) + ((float(df_pivot_high.iloc[x_2, 1]) - float(df_pivot_high.iloc[x_1, 1])) ** 2))
                        df_line_2.at[counter_lines, "x_1"] = df_pivot_high.iloc[x_1, 2]
                        df_line_2.at[counter_lines, "x_2"] = df_pivot_high.iloc[x_2, 2]
                        df_line_2.at[counter_lines, "y_1"] = float(df_pivot_high.iloc[x_1, 1])
                        df_line_2.at[counter_lines, "y_2"] = float(df_pivot_high.iloc[x_2, 1])
                        df_line_2.at[counter_lines, "index_1"] = float(df_pivot_high.iloc[x_1, 3])
                        df_line_2.at[counter_lines, "index_2"] = float(df_pivot_high.iloc[x_2, 3])
                        df_line_2.at[counter_lines, "S line"] = math.sqrt((((df_pivot_high.iloc[x_2, 3] / k_1) * k_2 - (df_pivot_high.iloc[x_1, 3] / k_1) * k_2) ** 2) + ((float(df_pivot_high.iloc[x_2, 1]) - float(df_pivot_high.iloc[x_1, 1])) ** 2))
                        counter_lines += 1
                except:
                    pass

        counter = 0
        for line_1 in range(df_line_1.shape[0]):
            for line_2 in range(df_line_2.shape[0]):
                try:
                    if line_1 < line_2 and df_line_1.at[line_1, "x_1"].timestamp() < df_line_2.at[line_2, "x_1"].timestamp() and df_line_1.at[line_1, "x_2"].timestamp() < df_line_2.at[line_2, "x_2"].timestamp() and df_line_1.at[line_1, "x_2"].timestamp() == df_line_2.at[line_2, "x_1"].timestamp():
                        length = math.sqrt((((df_line_2.at[line_2, "index_2"] / k_1) * k_2 - (df_line_1.at[line_1, "index_1"] / k_1) * k_2) ** 2) + ((float(df_line_2.at[line_2, "y_2"]) - float(df_line_1.at[line_1, "y_1"])) ** 2))
                        alpha = math.degrees(math.acos((df_line_1.at[line_1, "S line"] ** 2 + df_line_2.at[line_2, "S line"] ** 2 - length ** 2) / (2 * df_line_1.at[line_1, "S line"] * df_line_2.at[line_2, "S line"])))
                        an = ((df_line_1.at[line_1, "S line"] ** 2) + (df_line_2.at[line_2, "S line"] ** 2) - (length ** 2)) / (2 * df_line_1.at[line_1, "S line"] * df_line_2.at[line_2, "S line"])
                        if 177 <= alpha <= 180 and not (float(df_line_1.iloc[line_1, 2]) + (0.0035 * float(df_line_1.iloc[line_1, 2])) < float(df_line_2.iloc[line_2, 3]) and tf == "1h") and not (float(df_line_1.iloc[line_1, 2]) + (0.002 * float(df_line_1.iloc[line_1, 2])) < float(df_line_2.iloc[line_2, 3]) and tf == "15m") and not (float(df_line_1.iloc[line_1, 2]) + (0.001 * float(df_line_1.iloc[line_1, 2])) < float(df_line_2.iloc[line_2, 3]) and tf == "5m") and not (df_line_1.at[line_1, "index_1"] == df_line_1.at[line_1, "index_2"] - 1 == df_line_2.at[line_2, "index_1"] - 2):
                            #print(alpha)
                            #print(alpha, float(df_line_1.iloc[line_1, 2]), float(df_line_1.iloc[line_1, 3]), float(df_line_2.iloc[line_2, 2]), float(df_line_2.iloc[line_2, 3]), an, df_line_1.at[line_1, "S line"], df_line_2.at[line_2, "S line"], length, float(df_line_1.iloc[line_1, 2]), df_line_1.iloc[line_1, 0], df_line_1.iloc[line_1, 0].timestamp(), float(df_line_1.iloc[line_1, 3]), df_line_1.iloc[line_1, 1], df_line_1.iloc[line_1, 1].timestamp(), float(df_line_1.iloc[line_2, 2]), df_line_1.iloc[line_2, 0], df_line_1.iloc[line_2, 0].timestamp(), float(df_line_1.iloc[line_2, 3]), df_line_1.iloc[line_2, 1], df_line_1.iloc[line_2, 1].timestamp())
                            df_lines_high.at[counter, "x_1"] = df_line_1.iloc[line_1, 0]
                            df_lines_high.at[counter, "x_2"] = df_line_2.iloc[line_2, 1]
                            df_lines_high.at[counter, "y_1"] = float(df_line_1.iloc[line_1, 2])
                            df_lines_high.at[counter, "y_2"] = float(df_line_2.iloc[line_2, 3])
                            df_lines_high.at[counter, "index_1"] = float(df_line_1.iloc[line_1, 4])
                            df_lines_high.at[counter, "index_2"] = float(df_line_2.iloc[line_2, 5])
                            counter += 1
                except:
                    pass

        for j in range(df_lines_high.shape[0]):
            cross_counter = 0
            k_time = time.time()
            for k in range(int(df_lines_high.at[j, "index_1"]), df.shape[0]):
                price_line = (((df.at[k, 12] / k_1) * k_2 - (df_lines_high.at[j, "index_1"] / k_1) * k_2) * (float(df_lines_high.at[j, "y_2"]) - float(df_lines_high.at[j, "y_1"]))) / ((df_lines_high.at[j, "index_2"] / k_1) * k_2 - (df_lines_high.at[j, "index_1"] / k_1) * k_2) + float(df_lines_high.at[j, "y_1"])
                if price_line < float(df.at[k, 2]):
                    cross_counter += 1
                if cross_counter > 3:
                    df_lines_high.at[j, "cross"] = pd.NA
                    break
                elif cross_counter <= 3 and k == df.shape[0] - 1:
                    df_lines_high.at[j, "cross"] = cross_counter
        try:
            df_lines_high = df_lines_high.dropna(subset=["cross"])
        except:
            pass

        #_________________________________________________________________low____________________________________________________________

        df_pivot_low = pd.DataFrame()
        df_pivot_low["is_low"] = df[3].rolling(window, center=True).apply(lambda x: x[period] == min(x), raw=True)
        df_pivot_low["price"] = df[3][df_pivot_low["is_low"] == 1]
        df_pivot_low["time"] = df[0]
        df_pivot_low["index"] = df[12]
        df_pivot_low = df_pivot_low.dropna(subset=["price"])
        df_lines_low = pd.DataFrame()
        init = time.time()
        for j, row_1 in df_pivot_low.iterrows():
            for k, row_2 in df_pivot_low.iterrows():
                if pd.notna(df_pivot_low.at[j, 'price']) and pd.notna(df_pivot_low.at[k, 'price']):
                    if j < k and df_pivot_low.at[j, "price"] > df_pivot_low.at[k, "price"]:
                        df_pivot_low.at[j, 'price'] = pd.NA
        df_pivot_low = df_pivot_low.dropna(subset=["price"])

        df_line_3 = pd.DataFrame()
        df_line_4 = pd.DataFrame()
        counter_lines = 0
        for x_1 in range(df_pivot_low.shape[0]):
            for x_2 in range(df_pivot_low.shape[0]):
                try:
                    if x_1 < x_2:
                        df_line_3.at[counter_lines, "x_1"] = df_pivot_low.iloc[x_1, 2]
                        df_line_3.at[counter_lines, "x_2"] = df_pivot_low.iloc[x_2, 2]
                        df_line_3.at[counter_lines, "y_1"] = float(df_pivot_low.iloc[x_1, 1])
                        df_line_3.at[counter_lines, "y_2"] = float(df_pivot_low.iloc[x_2, 1])
                        df_line_3.at[counter_lines, "index_1"] = float(df_pivot_low.iloc[x_1, 3])
                        df_line_3.at[counter_lines, "index_2"] = float(df_pivot_low.iloc[x_2, 3])
                        df_line_3.at[counter_lines, "S line"] = math.sqrt((((df_pivot_low.iloc[x_2, 3] / k_1) * k_2 - (df_pivot_low.iloc[x_1, 3] / k_1) * k_2) ** 2) + ((float(df_pivot_low.iloc[x_2, 1]) - float(df_pivot_low.iloc[x_1, 1])) ** 2))
                        df_line_4.at[counter_lines, "x_1"] = df_pivot_low.iloc[x_1, 2]
                        df_line_4.at[counter_lines, "x_2"] = df_pivot_low.iloc[x_2, 2]
                        df_line_4.at[counter_lines, "y_1"] = float(df_pivot_low.iloc[x_1, 1])
                        df_line_4.at[counter_lines, "y_2"] = float(df_pivot_low.iloc[x_2, 1])
                        df_line_4.at[counter_lines, "index_1"] = float(df_pivot_low.iloc[x_1, 3])
                        df_line_4.at[counter_lines, "index_2"] = float(df_pivot_low.iloc[x_2, 3])
                        df_line_4.at[counter_lines, "S line"] = math.sqrt((((df_pivot_low.iloc[x_2, 3] / k_1) * k_2 - (df_pivot_low.iloc[x_1, 3] / k_1) * k_2) ** 2) + ((float(df_pivot_low.iloc[x_2, 1]) - float(df_pivot_low.iloc[x_1, 1])) ** 2))
                        counter_lines += 1
                except:
                    pass

        counter = 0
        for line_3 in range(df_line_3.shape[0]):
            for line_4 in range(df_line_4.shape[0]):
                try:
                    if line_3 < line_4 and df_line_3.at[line_3, "x_1"].timestamp() < df_line_4.at[line_4, "x_1"].timestamp() and df_line_3.at[line_3, "x_2"].timestamp() < df_line_4.at[line_4, "x_2"].timestamp() and df_line_3.at[line_3, "x_2"].timestamp() == df_line_4.at[line_4, "x_1"].timestamp():
                        length = math.sqrt((((df_line_4.at[line_4, "index_2"] / k_1) * k_2 - (df_line_3.at[line_3, "index_1"] / k_1) * k_2) ** 2) + ((float(df_line_4.at[line_4, "y_2"]) - float(df_line_3.at[line_3, "y_1"])) ** 2))
                        alpha = math.degrees(math.acos((df_line_3.at[line_3, "S line"] ** 2 + df_line_4.at[line_4, "S line"] ** 2 - length ** 2) / (2 * df_line_3.at[line_3, "S line"] * df_line_4.at[line_4, "S line"])))
                        an = ((df_line_3.at[line_3, "S line"] ** 2) + (df_line_4.at[line_4, "S line"] ** 2) - (length ** 2)) / (2 * df_line_3.at[line_3, "S line"] * df_line_4.at[line_4, "S line"])
                        if 177 <= alpha <= 180 and not (float(df_line_3.iloc[line_3, 2]) - (0.0035 * float(df_line_3.iloc[line_3, 2])) > float(df_line_4.iloc[line_4, 3]) and tf == "1h") and not (float(df_line_3.iloc[line_3, 2]) - (0.002 * float(df_line_3.iloc[line_3, 2])) > float(df_line_4.iloc[line_4, 3]) and tf == "15m") and not (float(df_line_3.iloc[line_3, 2]) - (0.001 * float(df_line_3.iloc[line_3, 2])) > float(df_line_4.iloc[line_4, 3]) and tf == "5m") and not (df_line_3.at[line_3, "index_1"] == df_line_3.at[line_3, "index_2"] - 1 == df_line_4.at[line_4, "index_1"] - 2):
                            # print(alpha)
                            # print(alpha, float(df_line_3.iloc[line_3, 2]), float(df_line_3.iloc[line_3, 3]), float(df_line_4.iloc[line_4, 2]), float(df_line_4.iloc[line_4, 3]), an, df_line_3.at[line_3, "S line"], df_line_4.at[line_4, "S line"], length, float(df_line_3.iloc[line_3, 2]), df_line_3.iloc[line_3, 0], df_line_3.iloc[line_3, 0].timestamp(), float(df_line_3.iloc[line_3, 3]), df_line_3.iloc[line_3, 1], df_line_3.iloc[line_3, 1].timestamp(), float(df_line_3.iloc[line_4, 2]), df_line_3.iloc[line_4, 0], df_line_3.iloc[line_4, 0].timestamp(), float(df_line_3.iloc[line_4, 3]), df_line_3.iloc[line_4, 1], df_line_3.iloc[line_4, 1].timestamp())
                            df_lines_low.at[counter, "x_1"] = df_line_3.iloc[line_3, 0]
                            df_lines_low.at[counter, "x_2"] = df_line_4.iloc[line_4, 1]
                            df_lines_low.at[counter, "y_1"] = float(df_line_3.iloc[line_3, 2])
                            df_lines_low.at[counter, "y_2"] = float(df_line_4.iloc[line_4, 3])
                            df_lines_low.at[counter, "index_1"] = float(df_line_3.iloc[line_3, 4])
                            df_lines_low.at[counter, "index_2"] = float(df_line_4.iloc[line_4, 5])
                            counter += 1
                except:
                    pass

        for j in range(df_lines_low.shape[0]):
            cross_counter = 0
            k_time = time.time()
            for k in range(int(df_lines_low.at[j, "index_1"]), df.shape[0]):
                price_line = (((df.at[k, 12] / k_1) * k_2 - (df_lines_low.at[j, "index_1"] / k_1) * k_2) * (float(df_lines_low.at[j, "y_2"]) - float(df_lines_low.at[j, "y_1"]))) / ((df_lines_low.at[j, "index_2"] / k_1) * k_2 - (df_lines_low.at[j, "index_1"] / k_1) * k_2) + float(df_lines_low.at[j, "y_1"])
                if price_line > float(df.at[k, 3]):
                    cross_counter += 1
                if cross_counter > 3:
                    df_lines_low.at[j, "cross"] = pd.NA
                    break
                elif cross_counter <= 3 and k == df.shape[0] - 1:
                    df_lines_low.at[j, "cross"] = cross_counter
        try:
            df_lines_low = df_lines_low.dropna(subset=["cross"])
        except:
            pass

        sum_levels = int(df_lines_high.shape[0]) + int(df_lines_low.shape[0])
        TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
        curdoc().theme = "dark_minimal"
        p.append(figure(x_axis_type="datetime", tools=TOOLS, height=290, title=t[i]['ticker']['ticker'] + " " + tf + "   " + (str(sum_levels)) + (" levels" if sum_levels != 1 else " level")))
        p[i].xaxis.major_label_orientation = pi / 4
        p[i].grid.grid_line_alpha = 0.3

        p[i].segment(df[0], df[2], df[0], df[3], color="#ff8c00")  # high = 2 low = 3
        p[i].vbar(df[0][inc], w, df[1][inc], df[4][inc], fill_color="#21262a", line_color="#ff8c00")
        p[i].vbar(df[0][dec], w, df[1][dec], df[4][dec], fill_color="#ff8c00", line_color="#ff8c00")
        # p[i].circle(df_pivot_high["time"], df_pivot_high["price"], radius=10 * w, color="white", alpha=0.5)
        # p[i].circle(df_pivot_low["time"], df_pivot_low["price"], radius=10 * w, color="white", alpha=0.5)
        #p[i].line(df_pivot_high["time"], df_pivot_high["price"], line_width=2, color="white")
        #p[i].line(df_pivot_low["time"], df_pivot_low["price"], line_width=2, color="white")
        for j in range(df_lines_high.shape[0]):
            #p[i].line(df_lines_high.iloc[j, [0, 1]], df_lines_high.iloc[j, [2, 3]], line_width=2, color=f"#{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}")
            p[i].line(df_lines_high.iloc[j, [0, 1]], df_lines_high.iloc[j, [2, 3]], line_width=2, color=f"#ffffff")
        for j in range(df_lines_low.shape[0]):
            #p[i].line(df_lines_low.iloc[j, [0, 1]], df_lines_low.iloc[j, [2, 3]], line_width=2, color=f"#{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}{random.randint(1, 9)}")
            p[i].line(df_lines_low.iloc[j, [0, 1]], df_lines_low.iloc[j, [2, 3]], line_width=2, color=f"#ffffff")

        draw = time.time()
        print("draw: ", draw - start)


host = "https://appscreener.herokuapp.com/"
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

url = 'spot/'
t_json = requests.request('GET', host + url, headers=headers)
t = t_json.json()
print(len(t))

host = "https://api.binance.com"
url = '/api/v3/klines'

p1h = []
p15m = []
p5m = []
#p = [p1h, p15m, p5m]


def x3_magic(i):
    global p1h, p15m, p5m

    magic(p1h, '1h', 0.5 * 60 * 60 * 1000, i)
    magic(p15m, '15m', 0.25 * 0.5 * 60 * 60 * 1000, i)
    magic(p5m, '5m', (0.25 * 0.5 * 60 * 60 * 1000) / 3, i)


if __name__ == '__main__':
    the_start = time.time()
    print(the_start)
    for i in range(len(t)):
        x3_magic(i)
        print("END: ", i, time.time() - the_start)
    print("FINAL: ", time.time() - the_start)

    show(row(column(p1h, sizing_mode='scale_width'), column(p15m, sizing_mode='scale_width'), column(p5m, sizing_mode='scale_width'), sizing_mode='scale_width'))
    print("FINAL show: ", time.time() - the_start)
