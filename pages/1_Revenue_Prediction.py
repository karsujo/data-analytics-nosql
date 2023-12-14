import streamlit as st
from datetime import datetime, tzinfo, timezone
import pandas as pd
from pymongo import MongoClient
import plotly.graph_objects as go
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px

# Page configurations
PAGE_CONFIG = {"page_title": "SMA Analysis", "page_icon": ":brain"}

# Set page title and icon
st.set_page_config(layout="wide", page_title=PAGE_CONFIG["page_title"], page_icon=PAGE_CONFIG["page_icon"])

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')

collection = client['cs532']['HotelWithReviews']


#Get top nationalities which stay at the hotel
topSpendingNationalityQuery = [
    {
        '$match': {
            'BookingsCheckedIn': {
                '$eq': 1
            }
        }
    }, {
        '$addFields': {
            'CalculatedMonth': {
                '$month': {
                    '$subtract': [
                        {
                            '$toDate': datetime.utcnow()
                        }, {
                            '$multiply': [
                                '$DaysSinceLastStay', 24 * 60 * 60 * 1000
                            ]
                        }
                    ]
                }
            }, 
            'CalculatedYear': {
                '$year': {
                    '$subtract': [
                        {
                            '$toDate': datetime.utcnow()
                        }, {
                            '$multiply': [
                                '$DaysSinceLastStay', 24 * 60 * 60 * 1000
                            ]
                        }
                    ]
                }
            }, 
            'calculatedDate': {
                '$subtract': [
                    {
                        '$toDate': datetime.utcnow()
                    }, {
                        '$multiply': [
                            '$DaysSinceCreation', 24 * 60 * 60 * 1000
                        ]
                    }
                ]
            }
        }
    }, {
        '$project': {
            '_id': 1, 
            'CalculatedMonth': '$CalculatedMonth', 
            'Nationality': '$Nationality', 
            'LodgingRevenue': '$LodgingRevenue', 
            'OtherRevenue': '$OtherRevenue'
        }
    }, {
        '$group': {
            '_id': {
                'CalculatedMonth': '$CalculatedMonth', 
                'Nationality': '$Nationality'
            }, 
            'count': {
                '$sum': 1
            }, 
            'LodgingRevenueForMonth': {
                '$sum': '$LodgingRevenue'
            }, 
            'OtherRevenueForMonth': {
                '$sum': '$OtherRevenue'
            }
        }
    }, {
        '$addFields': {
            'TotalRevenueForMonth': {
                '$add': [
                    '$OtherRevenueForMonth', '$LodgingRevenueForMonth'
                ]
            }
        }
    }, {
        '$sort': {
            '_id.CalculatedMonth': 1, 
            'TotalRevenueForMonth': -1
        }
    }, {
        '$group': {
            '_id': '$_id.CalculatedMonth', 
            'topNationalities': {
                '$push': '$_id.Nationality'
            }, 
            'topSpends': {
                '$push': '$TotalRevenueForMonth'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }, {
        '$project': {
            '_id': 0, 
            'month': '$_id', 
            'topNationalities': {
                '$slice': [
                    '$topNationalities', 2
                ]
            }, 
            'topSpending': {
                '$slice': [
                    '$topSpends', 2
                ]
            }
        }
    }
]

topSpendingNationality = list(collection.aggregate(topSpendingNationalityQuery))




#Get Training Data: Group by revenue and store in db
trainDataMontlyPipeline = [
    {
        '$addFields': {
            'CalculatedMonth': {
                '$month': {
                    '$subtract': [
                        {
                            '$toDate': datetime.utcnow()
                        }, {
                            '$multiply': [
                                '$DaysSinceLastStay', 24 * 60 * 60 * 1000
                            ]
                        }
                    ]
                }
            }, 
            'CalculatedYear': {
                '$year': {
                    '$subtract': [
                        {
                            '$toDate': datetime.utcnow()
                        }, {
                            '$multiply': [
                                '$DaysSinceLastStay', 24 * 60 * 60 * 1000
                            ]
                        }
                    ]
                }
            }, 
            'calculatedDate': {
                '$subtract': [
                    {
                        '$toDate': datetime.utcnow()
                    }, {
                        '$multiply': [
                            '$DaysSinceCreation', 24 * 60 * 60 * 1000
                        ]
                    }
                ]
            }
        }
    }, {
        '$match': {
            'BookingsCheckedIn': {
                '$gt': 0
            }
        }
    }, {
        '$project': {
            '_id': 1, 
            'CalculatedMonth': '$CalculatedMonth', 
            'CalculatedYear': '$CalculatedYear', 
            'Nationality': '$Nationality', 
            'LodgingRevenue': '$LodgingRevenue', 
            'OtherRevenue': '$OtherRevenue'
        }
    }, {
        '$group': {
            '_id': None, 
            'minDate': {
                '$min': '$CalculatedYear'
            }, 
            'maxDate': {
                '$max': '$CalculatedYear'
            }, 
            'data': {
                '$push': '$$ROOT'
            }
        }
    }, {
        '$unwind': {
            'path': '$data'
        }
    }, {
        '$set': {
            'count': {
                '$subtract': [
                    '$maxDate', '$minDate'
                ]
            }
        }
    }, {
        '$project': {
            'CalculatedMonth': '$data.CalculatedMonth', 
            'CalculatedYear': '$data.CalculatedYear', 
            'Nationality': '$data.Nationality', 
            'LodgingRevenue': '$data.LodgingRevenue', 
            'OtherRevenue': '$data.OtherRevenue', 
            'count': '$count'
        }
    }, {
        '$addFields': {
            'deviationFromRevenue': {
                '$multiply': [
                    {
                        '$rand': {}
                    }, 10
                ]
            }
        }
    }, {
        '$group': {
            '_id': '$CalculatedMonth', 
            'LodgingRevenueForMonth': {
                '$sum': '$LodgingRevenue'
            }, 
            'OtherRevenueForMonth': {
                '$sum': '$OtherRevenue'
            }, 
            'deviationFromRevenue': {
                '$sum': '$deviationFromRevenue'
            }, 
            'NumYears': {
                '$push': '$count'
            }
        }
    }, {
        '$addFields': {
            'TotalRevenue': {
                '$subtract': [
                    {
                        '$add': [
                            '$LodgingRevenueForMonth', '$OtherRevenueForMonth'
                        ]
                    }, '$deviationFromRevenue'
                ]
            }, 
            'NumYears': {
                '$arrayElemAt': [
                    '$NumYears', 0
                ]
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }, {
        '$project': {
            'Month': {
                '$cond': {
                    'if': {
                        '$lte': [
                            '$_id', 9
                        ]
                    }, 
                    'then': {
                        '$concat': [
                            '0', {
                                '$toString': '$_id'
                            }
                        ]
                    }, 
                    'else': {
                        '$toString': '$_id'
                    }
                }
            }, 
            'TotalCountOfRecordsForMonth': '$NumYears', 
            'LodgingRevenueForMonth': '$LodgingRevenueForMonth', 
            'OtherRevenueForMonth': '$OtherRevenueForMonth', 
            'TotalRevenue': '$TotalRevenue'
        }
    }, {
        '$merge': {
            'into': 'MonthlyTotalRevenue'
        }
    }
]

trainDataMontly = list(collection.aggregate(trainDataMontlyPipeline))

#Test data and predict next 6 month data
predictRevenuePipeline = [
    {
        '$addFields': {
            'CalculatedMonth': {
                '$let': {
                    'vars': {
                        'rawMonth': {
                            '$month': {
                                '$subtract': [
                                    {
                                        '$toDate': datetime.utcnow()
                                    }, {
                                        '$multiply': [
                                            '$DaysSinceLastStay', 24 * 60 * 60 * 1000
                                        ]
                                    }
                                ]
                            }
                        }
                    }, 
                    'in': {
                        '$cond': {
                            'if': {
                                '$lte': [
                                    '$$rawMonth', 9
                                ]
                            }, 
                            'then': {
                                '$concat': [
                                    '0', {
                                        '$toString': '$$rawMonth'
                                    }
                                ]
                            }, 
                            'else': {
                                '$toString': '$$rawMonth'
                            }
                        }
                    }
                }
            }, 
            'CalculatedYear': {
                '$year': {
                    '$subtract': [
                        {
                            '$toDate': datetime.utcnow()
                        }, {
                            '$multiply': [
                                '$DaysSinceLastStay', 24 * 60 * 60 * 1000
                            ]
                        }
                    ]
                }
            }, 
            'calculatedDate': {
                '$subtract': [
                    {
                        '$toDate': datetime.utcnow()
                    }, {
                        '$multiply': [
                            '$DaysSinceCreation', 24 * 60 * 60 * 1000
                        ]
                    }
                ]
            }
        }
    }, {
        '$match': {
            'BookingsCheckedIn': {
                '$eq': 1
            }
        }
    }, {
        '$group': {
            '_id': {
                'YearGroup': '$CalculatedYear', 
                'MonthGroup': '$CalculatedMonth'
            }, 
            'count': {
                '$sum': 1
            }, 
            'TotalLodgingRevenue': {
                '$sum': '$LodgingRevenue'
            }, 
            'TotalOtherRevenue': {
                '$sum': '$OtherRevenue'
            }
        }
    }, {
        '$addFields': {
            'TotalRevenue': {
                '$add': [
                    '$TotalLodgingRevenue', '$TotalOtherRevenue'
                ]
            }
        }
    }, {
        '$sort': {
            '_id.YearGroup': 1, 
            '_id.MonthGroup': 1
        }
    }, {
        '$group': {
            '_id': '1', 
            'YearAndMonthGroup': {
                '$push': {
                    '$concat': [
                        {
                            '$toString': '$_id.MonthGroup'
                        }, '-', {
                            '$toString': '$_id.YearGroup'
                        }
                    ]
                }
            }, 
            'revenue': {
                '$push': '$TotalRevenue'
            }
        }
    }, {
        '$addFields': {
            'SMA': {
                '$map': {
                    'input': {
                        '$range': [
                            0, {
                                '$size': '$revenue'
                            }
                        ]
                    }, 
                    'in': {
                        '$avg': {
                            '$slice': [
                                '$revenue', {
                                    '$cond': {
                                        'if': {
                                            '$lt': [
                                                {
                                                    '$subtract': [
                                                        {
                                                            '$add': [
                                                                '$$this', 1
                                                            ]
                                                        }, 6
                                                    ]
                                                }, 0
                                            ]
                                        }, 
                                        'then': 0, 
                                        'else': {
                                            '$subtract': [
                                                {
                                                    '$add': [
                                                        '$$this', 1
                                                    ]
                                                }, 6
                                            ]
                                        }
                                    }
                                }, 6
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$addFields': {
            'SMA': {
                '$concatArrays': [
                    '$SMA', {
                        '$map': {
                            'input': {
                                '$range': [
                                    {
                                        '$subtract': [
                                            {
                                                '$size': '$SMA'
                                            }, 6
                                        ]
                                    }, {
                                        '$add': [
                                            {
                                                '$size': '$SMA'
                                            }, 0
                                        ]
                                    }
                                ]
                            }, 
                            'in': {
                                '$avg': {
                                    '$slice': [
                                        '$revenue', '$$this', 6
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        }
    }, {
        '$set': {
            'lastElement': {
                '$arrayElemAt': [
                    '$YearAndMonthGroup', -1
                ]
            }
        }
    }, {
        '$set': {
            'nextMonthYear': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': {
                        '$dateFromParts': {
                            'year': {
                                '$toInt': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$lastElement', '-'
                                            ]
                                        }, 1
                                    ]
                                }
                            }, 
                            'month': {
                                '$add': [
                                    {
                                        '$toInt': {
                                            '$arrayElemAt': [
                                                {
                                                    '$split': [
                                                        '$lastElement', '-'
                                                    ]
                                                }, 0
                                            ]
                                        }
                                    }, 1
                                ]
                            }, 
                            'day': 1
                        }
                    }
                }
            }
        }
    }, {
        '$set': {
            'YearAndMonthGroup': {
                '$concatArrays': [
                    '$YearAndMonthGroup', [
                        '$nextMonthYear'
                    ]
                ]
            }
        }
    }, {
        '$set': {
            'lastElement': {
                '$arrayElemAt': [
                    '$YearAndMonthGroup', -1
                ]
            }
        }
    }, {
        '$set': {
            'nextMonthYear': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': {
                        '$dateFromParts': {
                            'year': {
                                '$toInt': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$lastElement', '-'
                                            ]
                                        }, 1
                                    ]
                                }
                            }, 
                            'month': {
                                '$add': [
                                    {
                                        '$toInt': {
                                            '$arrayElemAt': [
                                                {
                                                    '$split': [
                                                        '$lastElement', '-'
                                                    ]
                                                }, 0
                                            ]
                                        }
                                    }, 1
                                ]
                            }, 
                            'day': 1
                        }
                    }
                }
            }
        }
    }, {
        '$set': {
            'YearAndMonthGroup': {
                '$concatArrays': [
                    '$YearAndMonthGroup', [
                        '$nextMonthYear'
                    ]
                ]
            }
        }
    }, {
        '$set': {
            'lastElement': {
                '$arrayElemAt': [
                    '$YearAndMonthGroup', -1
                ]
            }
        }
    }, {
        '$set': {
            'nextMonthYear': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': {
                        '$dateFromParts': {
                            'year': {
                                '$toInt': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$lastElement', '-'
                                            ]
                                        }, 1
                                    ]
                                }
                            }, 
                            'month': {
                                '$add': [
                                    {
                                        '$toInt': {
                                            '$arrayElemAt': [
                                                {
                                                    '$split': [
                                                        '$lastElement', '-'
                                                    ]
                                                }, 0
                                            ]
                                        }
                                    }, 1
                                ]
                            }, 
                            'day': 1
                        }
                    }
                }
            }
        }
    }, {
        '$set': {
            'YearAndMonthGroup': {
                '$concatArrays': [
                    '$YearAndMonthGroup', [
                        '$nextMonthYear'
                    ]
                ]
            }
        }
    }, {
        '$set': {
            'lastElement': {
                '$arrayElemAt': [
                    '$YearAndMonthGroup', -1
                ]
            }
        }
    }, {
        '$set': {
            'nextMonthYear': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': {
                        '$dateFromParts': {
                            'year': {
                                '$toInt': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$lastElement', '-'
                                            ]
                                        }, 1
                                    ]
                                }
                            }, 
                            'month': {
                                '$add': [
                                    {
                                        '$toInt': {
                                            '$arrayElemAt': [
                                                {
                                                    '$split': [
                                                        '$lastElement', '-'
                                                    ]
                                                }, 0
                                            ]
                                        }
                                    }, 1
                                ]
                            }, 
                            'day': 1
                        }
                    }
                }
            }
        }
    }, {
        '$set': {
            'YearAndMonthGroup': {
                '$concatArrays': [
                    '$YearAndMonthGroup', [
                        '$nextMonthYear'
                    ]
                ]
            }
        }
    }, {
        '$set': {
            'lastElement': {
                '$arrayElemAt': [
                    '$YearAndMonthGroup', -1
                ]
            }
        }
    }, {
        '$set': {
            'nextMonthYear': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': {
                        '$dateFromParts': {
                            'year': {
                                '$toInt': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$lastElement', '-'
                                            ]
                                        }, 1
                                    ]
                                }
                            }, 
                            'month': {
                                '$add': [
                                    {
                                        '$toInt': {
                                            '$arrayElemAt': [
                                                {
                                                    '$split': [
                                                        '$lastElement', '-'
                                                    ]
                                                }, 0
                                            ]
                                        }
                                    }, 1
                                ]
                            }, 
                            'day': 1
                        }
                    }
                }
            }
        }
    }, {
        '$set': {
            'YearAndMonthGroup': {
                '$concatArrays': [
                    '$YearAndMonthGroup', [
                        '$nextMonthYear'
                    ]
                ]
            }
        }
    }, {
        '$set': {
            'lastElement': {
                '$arrayElemAt': [
                    '$YearAndMonthGroup', -1
                ]
            }
        }
    }, {
        '$set': {
            'nextMonthYear': {
                '$dateToString': {
                    'format': '%m-%Y', 
                    'date': {
                        '$dateFromParts': {
                            'year': {
                                '$toInt': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$lastElement', '-'
                                            ]
                                        }, 1
                                    ]
                                }
                            }, 
                            'month': {
                                '$add': [
                                    {
                                        '$toInt': {
                                            '$arrayElemAt': [
                                                {
                                                    '$split': [
                                                        '$lastElement', '-'
                                                    ]
                                                }, 0
                                            ]
                                        }
                                    }, 1
                                ]
                            }, 
                            'day': 1
                        }
                    }
                }
            }
        }
    }, {
        '$set': {
            'YearAndMonthGroup': {
                '$concatArrays': [
                    '$YearAndMonthGroup', [
                        '$nextMonthYear'
                    ]
                ]
            }
        }
    }, {
        '$set': {
            'revenue': {
                '$concatArrays': [
                    '$revenue', [
                        0, 0, 0, 0, 0, 0
                    ]
                ]
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'zip': {
                '$zip': {
                    'inputs': [
                        '$YearAndMonthGroup', '$revenue', '$SMA'
                    ]
                }
            }
        }
    }, {
        '$unwind': {
            'path': '$zip', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$project': {
            'Month': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            {
                                '$arrayElemAt': [
                                    '$zip', 0
                                ]
                            }, '-'
                        ]
                    }, 0
                ]
            }, 
            'MonthYear': {
                '$arrayElemAt': [
                    '$zip', 0
                ]
            }, 
            'ActualRevenue': {
                '$arrayElemAt': [
                    '$zip', 1
                ]
            }, 
            'ProjectedRevenue': {
                '$arrayElemAt': [
                    '$zip', 2
                ]
            }
        }
    }, {
        '$lookup': {
            'from': 'MonthlyTotalRevenue', 
            'localField': 'Month', 
            'foreignField': 'Month', 
            'as': 'result'
        }
    }, {
        '$unwind': {
            'path': '$result'
        }
    }, {
        '$addFields': {
            'ProjectedRevenue': {
                '$divide': [
                    {
                        '$add': [
                            '$ProjectedRevenue', '$result.TotalRevenue'
                        ]
                    }, {
                        '$add': [
                            '$result.TotalCountOfRecordsForMonth', 1
                        ]
                    }
                ]
            }
        }
    }, {
        '$project': {
            'Month': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$MonthYear', '-'
                        ]
                    }, 0
                ]
            }, 
            'MonthYear': '$MonthYear', 
            'ActualRevenue': '$ActualRevenue', 
            'ProjectedRevenue': '$ProjectedRevenue'
        }
    }
]

predictedActualData = list(collection.aggregate(predictRevenuePipeline))

predictedActualRevenueData = [entry for entry in predictedActualData if entry['ActualRevenue'] > 0]  # You can adjust the condition as needed

month_years = [entry['MonthYear'] for entry in predictedActualData]
actual_revenue = [entry['ActualRevenue'] for entry in predictedActualRevenueData]
projected_revenue = [entry['ProjectedRevenue'] for entry in predictedActualData]

fig = go.Figure()

# Add traces for actual and projected revenue
fig.add_trace(go.Scatter(x=month_years, y=actual_revenue, mode='lines+markers', name='Actual Revenue'))
fig.add_trace(go.Scatter(x=month_years, y=projected_revenue, mode='lines+markers', name='Projected Revenue'))

# Customize the layout
fig.update_layout(
    title='Actual vs Projected Revenue Over Months',
    xaxis_title='Month-Year',
    yaxis_title='Revenue',
    xaxis=dict(tickangle=45),
    hovermode='closest',
    height=750,
    width=1000
)

st.plotly_chart(fig)



# print(topSpendingNationality)

df = pd.DataFrame([(month, nat, spend) for entry in topSpendingNationality for month, nats, spends in [(entry['month'], entry['topNationalities'], entry['topSpending'])] for nat, spend in zip(nats, spends)], columns=['Month', 'Nationality', 'Spending'])

# Create an interactive bar chart
fig = px.bar(df, x='Nationality', y='Spending', color='Month', title='Range of Spending by Nationality Each Month',
             labels={'Spending': 'Spending Amount', 'Month': 'Month Number'}, barmode='group')

# Show the interactive chart using Streamlit
# st.plotly_chart(fig)

#########################################
# months = [entry['month'] for entry in topSpendingNationality]
# top_nationalities = [entry['topNationalities'][0] for entry in topSpendingNationality]
# top_spending = [entry['topSpending'][0] for entry in topSpendingNationality]

# fig = px.scatter(topSpendingNationality, x=months, y=top_spending, color=top_nationalities, labels={'x': 'Month', 'y': 'Top Spending'},
#                  title='Top Nationalities and Spending by Month')
# st.plotly_chart(fig)

months = [entry['month'] for entry in topSpendingNationality]
top_nationalities_0 = [entry['topNationalities'][0] for entry in topSpendingNationality]
top_spending_0 = [entry['topSpending'][0] for entry in topSpendingNationality]
top_nationalities_1 = [entry['topNationalities'][1] for entry in topSpendingNationality]
top_spending_1 = [entry['topSpending'][1] for entry in topSpendingNationality]

fig = go.Figure()

# Add a trace for each nationality in the first entry
for nationality in set(top_nationalities_0):
    indices = [i for i, value in enumerate(top_nationalities_0) if value == nationality]
    fig.add_trace(go.Scatter(x=[months[i] for i in indices],
                             y=[top_spending_0[i] for i in indices],
                             mode='markers',
                             name=f'{nationality}'))

# Add a trace for each nationality in the second entry
for nationality in set(top_nationalities_1):
    indices = [i for i, value in enumerate(top_nationalities_1) if value == nationality]
    print([months[i] for i in indices])
    fig.add_trace(go.Scatter(x=[months[i] for i in indices],
                             y=[top_spending_1[i] for i in indices],
                             mode='markers',
                             name=f'{nationality}'))

fig.update_layout(title='Top Nationalities and Spending by Month',
                  xaxis=dict(title='Month', tickmode='array', tickvals=months),
                  yaxis=dict(title='Top Spending'))

# st.plotly_chart(fig)