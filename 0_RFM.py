import streamlit as st
from datetime import datetime, tzinfo, timezone
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import pandas as pd
import numpy as np

# Page configurations
PAGE_CONFIG = {"page_title": "RFM Analysis", "page_icon": ":pick:"}

# Set page title and icon
st.set_page_config(layout="wide", page_title=PAGE_CONFIG["page_title"], page_icon=PAGE_CONFIG["page_icon"])

# Sidebar menu
# menu_options = ["Page 1", "Page 2", "Page 3"]
# selected_page = st.sidebar.radio("Select a Page", menu_options)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['cs532']['HotelWithReviews']

total_customers = num_documents = db.count_documents({}); 

#------------------------------------------------------------------------------------------------------------------------------------------
# Code for Data Table 
#------------------------------------------------------------------------------------------------------------------------------------------
pipeline = [
    {
        "$addFields": {
            "d_Recency": {
                "$add": [
                    {"$multiply": [0.1, {"$toInt": "$DaysSinceLastStay"}]},
                    {"$multiply": [0.5, {"$toInt": "$AverageLeadTime"}]}
                ]
            },
            "d_Frequency": {
                "$add": [
                    {"$multiply": [0.4, {"$toInt": "$DaysSinceFirstStay"}]},
                    {"$multiply": [0.5, {"$toInt": "$BookingsCanceled"}]},
                    {"$multiply": [0.3, {"$toInt": "$BookingsCheckedIn"}]},
                    {"$multiply": [0.35, {"$toInt": "$BookingsNoShowed"}]}
                ]
            },
            "d_Monetization": {
                "$add": [
                    {"$multiply": [0.4, {"$toDouble": "$LodgingRevenue"}]},
                    {"$multiply": [0.7, {"$toDouble": "$OtherRevenue"}]}
                ]
            },
            "customer_rank": {
                "$add": [
                    {"$multiply": [0.6, {"$subtract": [1, "$d_Recency"]}]},
                    {"$multiply": [0.2, "$d_Frequency"]},
                    {"$multiply": [0.2, "$d_Monetization"]}
                ]
            }
        }
    },
    {
        "$addFields": {
            "customer_rank": {
                "$add": [
                    {"$multiply": [1, "$d_Frequency"]},
                    {"$multiply": [1, "$d_Monetization"]},
                    {"$multiply": [-1, "$d_Recency"]}
                ]
            }
        }
    },
    {
        "$sort": {"customer_rank": 1}
    },
    {
        "$setWindowFields": {
            "sortBy": {"customer_rank": 1},
            "output": {"sorted_row_number": {"$documentNumber": {}}}
        }
    },
    {
        "$addFields": {
            "Segment_Rank": {
                "$switch": {
                    "branches": [
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.2, total_customers]}]}, "then": 1},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.4, total_customers]}]}, "then": 2},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.6, total_customers]}]}, "then": 3},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.8, total_customers]}]}, "then": 4},
                        {"case": "True", "then": 5}
                    ]
                }
            }
        }
    },
    {
        "$project": {
            "DocIDHash": 0,
            "NameHash": 0
        }
    }
]

# Execute the pipeline and get the result
result = list(db.aggregate(pipeline))

COLUMNS = [
    "ID", "Nationality", "Age", "d_Recency", "d_Frequency", "d_Monetization", "customer_rank", "Segment_Rank",
    "DaysSinceCreation", "AverageLeadTime",
    "LodgingRevenue", "OtherRevenue", "BookingsCanceled", "BookingsNoShowed", "BookingsCheckedIn",
    "PersonsNights", "RoomNights", "DaysSinceLastStay", "DaysSinceFirstStay", "DistributionChannel",
    "MarketSegment", "SRHighFloor", "SRLowFloor", "SRAccessibleRoom", "SRMediumFloor", "SRBathtub",
    "SRShower", "SRCrib", "SRKingSizeBed", "SRTwinBed", "SRNearElevator", "SRAwayFromElevator",
    "SRNoAlcoholInMiniBar", "SRQuietRoom", "CustomerReview", 
]

# Convert the result to a Pandas DataFrame
df = pd.DataFrame(result, columns=COLUMNS)
df = df.dropna()
# df_sorted = df.sort_values(by="Segment_Rank")

# filtered_df = df[df['Segment_Rank'] == 2]

# Display the first 1000 rows in a Streamlit table
st.title("RFM Segmentation Analysis Data")


st.dataframe(df.head(1000).style.set_table_styles([{
    'selector': 'thead th',
    'props': [('background-color', '#48D1CC'), ('color', 'white')]
}]), None, 200, use_container_width=True)


#------------------------------------------------------------------------------------------------------------------------------------------
# Code for Plot
#------------------------------------------------------------------------------------------------------------------------------------------

st.title("RFM Segmentation Analysis Plot")

pipeline_plot = [
    {
        "$addFields": {
            "d_Recency": {
                "$add": [
                    {"$multiply": [0.1, {"$toInt": "$DaysSinceLastStay"}]},
                    {"$multiply": [0.5, {"$toInt": "$AverageLeadTime"}]}
                ]
            },
            "d_Frequency": {
                "$add": [
                    {"$multiply": [0.4, {"$toInt": "$DaysSinceFirstStay"}]},
                    {"$multiply": [0.5, {"$toInt": "$BookingsCanceled"}]},
                    {"$multiply": [0.3, {"$toInt": "$BookingsCheckedIn"}]},
                    {"$multiply": [0.35, {"$toInt": "$BookingsNoShowed"}]}
                ]
            },
            "d_Monetization": {
                "$add": [
                    {"$multiply": [0.4, {"$toDouble": "$LodgingRevenue"}]},
                    {"$multiply": [0.7, {"$toDouble": "$OtherRevenue"}]}
                ]
            },
            "customer_rank": {
                "$add": [
                    {"$multiply": [0.6, {"$subtract": [1, "$d_Recency"]}]},
                    {"$multiply": [0.2, "$d_Frequency"]},
                    {"$multiply": [0.2, "$d_Monetization"]}
                ]
            }
        }
    },
    {
        "$addFields": {
            "customer_rank": {
                "$add": [
                    {"$multiply": [1, "$d_Frequency"]},
                    {"$multiply": [1, "$d_Monetization"]},
                    {"$multiply": [-1, "$d_Recency"]}
                ]
            }
        }
    },
    {
        "$sort": {"customer_rank": -1}
    },
    {
        "$setWindowFields": {
            "sortBy": {"customer_rank": -1},
            "output": {"sorted_row_number": {"$documentNumber": {}}}
        }
    },
    {
        "$addFields": {
            "Segment_Rank": {
                "$switch": {
                    "branches": [
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.2, total_customers]}]}, "then": 1},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.4, total_customers]}]}, "then": 2},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.6, total_customers]}]}, "then": 3},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.8, total_customers]}]}, "then": 4},
                        {"case": "True", "then": 5}
                    ]
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$Segment_Rank",
            "averageLodgingRevenue": {"$avg": "$LodgingRevenue"},
            "averageOtherRevenue" : {"$avg": "$OtherRevenue"},
            "averageLeadTime": {"$avg":"$AverageLeadTime"},
            "averageDaysSinceFirstStay": {"$avg":"$DaysSinceFirstStay"},
            "averageRoomNights": {"$avg":"$RoomNights"}

        }
    },
    {
        "$project": {
            "DocIDHash": 0,
            "NameHash": 0
        }
    }
]

# Execute the pipeline and get the result
result_plot = list(db.aggregate(pipeline_plot))

# print(result_plot)
# Convert the MongoDB result to a Pandas DataFrame
df_plot = pd.DataFrame(result_plot)


# Display the original data
# st.subheader("Original Data:")
# st.write(df_plot)

# Plot the bar graph using Plotly and display it in Streamlit
# fig_plot = px.bar(df_plot, x="_id", y=["averageLodgingRevenue","averageOtherRevenue"], labels={"_id": "Segment Rank", "averageLodgingRevenue": "Average Lodging Revenue", "averageOtherRevenue": "Average Other Revenue"})
# fig_plot.update_layout(title="Average Lodging Revenue by Segment Rank", xaxis_title="Segment Rank", yaxis_title="Average Revenue")
# st.plotly_chart(fig_plot)

fig_plot = go.Figure()

fig_plot.add_trace(go.Bar(x=df_plot["_id"], y=df_plot["averageLodgingRevenue"], name="Average Lodging Revenue"))
fig_plot.add_trace(go.Bar(x=df_plot["_id"], y=df_plot["averageDaysSinceFirstStay"], name="Average Days Since First Stay"))
fig_plot.add_trace(go.Bar(x=df_plot["_id"], y=df_plot["averageOtherRevenue"], name="Average Other Revenue"))
fig_plot.add_trace(go.Bar(x=df_plot["_id"], y=df_plot["averageLeadTime"], name="Average Lead Time"))
fig_plot.add_trace(go.Bar(x=df_plot["_id"], y=df_plot["averageRoomNights"], name="Average Room Nights"))

fig_plot.update_layout(
    title="Average Revenue by Segment Rank",
    xaxis_title="Segment Rank", yaxis_title="Average Revenue",
    scene=dict(
        xaxis=dict(title="Segment Rank"),
        yaxis=dict(title="Revenue Type"),
        zaxis=dict(title="Average Revenue"),
    )
)

st.plotly_chart(fig_plot)

#------------------------------------------------------------------------------------------------------------------------------------------
# Code for 3-D Plot
#------------------------------------------------------------------------------------------------------------------------------------------
st.title("RFM Segmentation Analysis 3D Plot")

pipeline_plot_3d = [
    {
        "$addFields": {
            "d_Recency": {
                "$add": [
                    {"$multiply": [0.1, {"$toInt": "$DaysSinceLastStay"}]},
                    {"$multiply": [0.5, {"$toInt": "$AverageLeadTime"}]}
                ]
            },
            "d_Frequency": {
                "$add": [
                    {"$multiply": [0.4, {"$toInt": "$DaysSinceFirstStay"}]},
                    {"$multiply": [0.5, {"$toInt": "$BookingsCanceled"}]},
                    {"$multiply": [0.3, {"$toInt": "$BookingsCheckedIn"}]},
                    {"$multiply": [0.35, {"$toInt": "$BookingsNoShowed"}]}
                ]
            },
            "d_Monetization": {
                "$add": [
                    {"$multiply": [0.4, {"$toDouble": "$LodgingRevenue"}]},
                    {"$multiply": [0.7, {"$toDouble": "$OtherRevenue"}]}
                ]
            },
            "customer_rank": {
                "$add": [
                    {"$multiply": [0.6, {"$subtract": [1, "$d_Recency"]}]},
                    {"$multiply": [0.2, "$d_Frequency"]},
                    {"$multiply": [0.2, "$d_Monetization"]}
                ]
            }
        }
    },
    {
        "$addFields": {
            "customer_rank": {
                "$add": [
                    {"$multiply": [1, "$d_Frequency"]},
                    {"$multiply": [1, "$d_Monetization"]},
                    {"$multiply": [-1, "$d_Recency"]}
                ]
            }
        }
    },
    {
        "$sort": {"customer_rank": -1}
    },
    {
        "$setWindowFields": {
            "sortBy": {"customer_rank": -1},
            "output": {"sorted_row_number": {"$documentNumber": {}}}
        }
    },
    {
        "$addFields": {
            "Segment_Rank": {
                "$switch": {
                    "branches": [
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.2, total_customers]}]}, "then": 1},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.4, total_customers]}]}, "then": 2},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.6, total_customers]}]}, "then": 3},
                        {"case": {"$lte": ["$sorted_row_number", {"$multiply": [0.8, total_customers]}]}, "then": 4},
                        {"case": "True", "then": 5}
                    ]
                }
            }
        }
    },
    {
        "$group": {

            "_id": {"Segment_Rank": "$Segment_Rank", "DistributionChannel": "$DistributionChannel"},
            "lodgingRevenuePerChannel": {"$sum": "$LodgingRevenue"}
    
        }
    },
    {
        "$project": {
            "Segment_Rank": "$_id.Segment_Rank",
            "LodgingRevenue":"$lodgingRevenuePerChannel",
            "DistributionChannel":"$_id.DistributionChannel"
        }
    }
]

# Execute the pipeline and get the result
result_plot_3d = list(db.aggregate(pipeline_plot_3d))


# Convert the MongoDB result to a Pandas DataFrame
df_plot_3d= pd.DataFrame(result_plot_3d)

fig_3d = px.scatter_3d(df_plot_3d, 
                      x='Segment_Rank', 
                      y='LodgingRevenue', 
                      z='DistributionChannel', 
                      size='Segment_Rank',
                    #   color='Segment_Rank', 
                      opacity=0.7, 
                      labels={'Segment_Rank': 'Segment_Rank'})


# Streamlit app
# st.title('3D Plot for Segment Rank, Lodging Revenue, and Distribution Channel')
st.plotly_chart(fig_3d)

