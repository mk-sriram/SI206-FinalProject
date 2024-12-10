#import
import plotly.express as px
import json
from processData import calc_linear_regression
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

#making functions for organization
# Open and load the JSON file
def load_data():
    with open("process_data.json", "r") as file:
        data = json.load(file)
    return data
    #print(data["data"][0]['total_points']) #how to access a single value

#initialize lists
def initialize_dict():
    data = load_data()
    total_points        = []
    temperature_celsius = []
    precipitation       = []
    wind_speed          = []

    #initializing total_points
    for i in range(len(data["data"])):
        total_points.append(data["data"][i]['total_points'])
    #initializing temperature_celsius
    for i in range(len(data["data"])):
        temperature_celsius.append(data["data"][i]['temperature'])
    #intializing precipitation
    for i in range(len(data["data"])):
        precipitation.append(data["data"][i]['precipitation'])
    #intializing wind_speed
    for i in range(len(data["data"])):
        wind_speed.append(data["data"][i]['wind_speed'])
    return (total_points, temperature_celsius, precipitation, wind_speed)

#prepping for plotting
def set_plot_data():
    vals = initialize_dict()
    plot_data = {}

    plot_data['total_points'] = vals[0]
    plot_data['temp'] = vals[1]
    plot_data['prec'] = vals[2]
    plot_data['wind'] = vals[3]

    return (plot_data, vals)

#for a diff plot calculating average for a bar graph
def linear_regression(vals):
    temp_line_reg = calc_linear_regression(vals, 1, 0)
    temp_line_reg["label"] = "Temperature"
    prec_line_reg = calc_linear_regression(vals, 2, 0)
    prec_line_reg["label"] = "Precipitation"
    wind_line_reg = calc_linear_regression(vals, 3, 0)
    wind_line_reg["label"] = "Wind_Speed"

    return(temp_line_reg, prec_line_reg, wind_line_reg)

#drawing up all plots
def make_plot():
    plot_data = set_plot_data()[0]
    vals = initialize_dict()
    linear_regressions = linear_regression(vals)

    tailored_data = []
    x_values = np.linspace(-10, 10, 100)  # Define the range of x-values

    for line in linear_regressions:
        y_values = line["slope"] * x_values + line["intercept"]
        for x, y in zip(x_values, y_values):
            tailored_data.append({"x": x, "y": y , "line": line["label"]})  # Include line label , "line": line["label"]

    df = pd.DataFrame(tailored_data)

    # Plot all lines on the same plot using plotly.express
    fig_threeRegressions = px.line(df,
                                x="x", 
                                y="y", 
                                color="line", 
                                title="Linear Regressions Slopes Line Graph"
                                )

    fig_totalPoints_vs_temp = px.scatter(
                            x=plot_data['temp'], 
                            y=plot_data['total_points'], 
                            title="Total Points Scored vs Temperature(celsius)",
                            labels={"x": "Temperature (Celsius)", "y": "Total Points Scored"}  # Label the axes
                            )

    fig_totalPoints_vs_prec = px.scatter(
                            x=plot_data['prec'], 
                            y=plot_data['total_points'], 
                            title="Total Points Scored vs Precipitation",
                            labels={"x": "Preciptiation", "y": "Total Points Scored"}  # Label the axes
                            )

    fig_totalPoints_vs_wind = px.scatter(
                            x=plot_data['wind'], 
                            y=plot_data['total_points'], 
                            title="Total Points Scored vs Wind Speed",
                            labels={"x": "Wind Speed", "y": "Total Points Scored"}  # Label the axes
                            )

    #bar plot calcs
    bar_plot_data = {
                    "Category": ["Temperature", "Precipitation", "Wind_Speed"],
                    "Averages": [linear_regressions[0]['slope'],
                                 linear_regressions[1]['slope'],
                                 linear_regressions[2]['slope']],
                    "line"    : [linear_regressions[0]['label'],
                                 linear_regressions[1]['label'],
                                 linear_regressions[2]['label']]
    }

    #data frame for bar plot
    df = pd.DataFrame(bar_plot_data)

    #bar plot
    fig_bar_plot = px.bar(df, 
                 x="Category", 
                 y="Averages", 
                 color="line", 
                 title="Linear Regressions by Category"
                 )

    fig = make_subplots(rows=5, 
                    cols=1, 
                    subplot_titles=("Total Points Scored vs Temperature(celsius)", 
                                                    "Total Points Scored vs Precipitation",
                                                    "Total Points Scored vs Wind Speed",
                                                    "Linear Regressions Slopes",
                                                    "Linear Regression Slopes Bar Plot"),
                    shared_yaxes= False                                
                                                    )

    fig.add_trace(fig_totalPoints_vs_temp.data[0], row=1, col=1)  # Add scatter data
    fig.add_trace(fig_totalPoints_vs_prec.data[0], row=2, col=1)      # Add bar data
    fig.add_trace(fig_totalPoints_vs_wind.data[0], row=3, col=1)     # Add line data
    #adding linear regressions in slope form
    for trace in fig_threeRegressions.data:
        fig.add_trace(trace, row=4, col=1)
    #adding linear regressions by category
    for trace in fig_bar_plot.data:
        fig.add_trace(trace, row=5, col=1)

    fig.update_layout(height=900, width=1200, title_text="Total Points scored vs conditions")

    fig.show()

#calling make_plot
make_plot()


