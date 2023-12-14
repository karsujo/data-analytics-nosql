from pymongo import MongoClient
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import streamlit as st
from datetime import datetime, tzinfo, timezone
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Page configurations
PAGE_CONFIG = {"page_title": "Word Analysis", "page_icon": ":brain"}

# Set page title and icon
st.set_page_config(layout="wide", page_title=PAGE_CONFIG["page_title"], page_icon=PAGE_CONFIG["page_icon"])

# Sidebar menu
# menu_options = ["Page 1", "Page 2", "Page 3"]
# selected_page = st.sidebar.radio("Select a Page", menu_options)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['cs532']['HotelWithReviews']

# Your MongoDB aggregation query
pipeline = [
    {
        "$project": {
            "words": {"$split": [{'$toString':"$CustomerReview"}, " "]}
        }
    },
    {
        "$unwind": "$words"
    },
    {
        "$group": {
            "_id": "$words",
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"count": -1}
    },
    {
        "$limit": 5000
    }
]

result = list(db.aggregate(pipeline))

# Extract words and counts from the result
words = [item['_id'] for item in result if item['_id'] not in ['a', 'an', 'the', 'in', 'on', 'at', 'for', 'to', 'with', 'by', 'of','for','I','was','and','The','were','is','that','you','have','but','so','it','as','we','very','room','not','staff','No','from','My','t','hotel','location','had','Location','brekfast','are','be','small','my','Negative']]
counts = [item['count'] for item in result]

# Create a dictionary with words and their counts
word_count_dict = dict(zip(words, counts))

wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_count_dict)



pipeline = [
    {
        "$project": {
            "words": {"$split": [{'$toString':"$CustomerReview"}, " "]}
        }
    },
    {
        "$unwind": "$words"
    },
    {
        "$group": {
            "_id": "$words",
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"count": -1}
    },
    {
        "$limit": 100
    }
]

result = list(db.aggregate(pipeline))
result = [doc for doc in result if doc['_id'] and doc['_id'].strip()]

exclude_list = ['Negative', 'no', 's', 'I', 'No', 'i', 'a', 'an', 'the', 'in', 'on', 'at', 'for', 'to', 'with', 'by', 'of', 'for', 'I', 'was', 'and', 'The', 'were', 'is', 'that', 'you', 'have', 'but', 'so', 'it', 'as', 'we', 'very', 'room', 'not', 'staff', 'No', 'from', 'My', 't', 'hotel', 'location', 'had', 'Location', 'brekfast', 'are', 'be', 'small', 'my', ]

# Remove documents with empty or null '_id' field and exclude specific words
result = [doc for doc in result if doc['_id'] and doc['_id'].strip() and doc['_id'].lower() not in exclude_list]


st.title("Word Frequency Analysis")

# Plot the WordCloud image using Streamlit
st.image(wordcloud.to_image(),use_column_width=True)

# Plot a bar chart using Plotly
fig = px.bar(result, x='_id', y='count', title='Word Frequency Bar Chart', labels={'_id': 'Word', 'count': 'Frequency'})
st.plotly_chart(fig, use_container_width=True)


#----------------------------------------------------------------------------------------------

word_count_pipeline = [
    {
        '$match': {
            '$expr': {
                '$and': [
                    { '$gte': [{ '$toDouble': "$Age" }, 20] },
                    { '$lte': [{ '$toDouble': "$Age" }, 40] }
                ]
            }
        }
    }, {
        '$project': {
            'CustomerReview': {
                '$toString': '$CustomerReview'
            }
        }
    }, {
        '$project': {
            'top_words': {
                '$split': [
                    '$CustomerReview', ' '
                ]
            }
        }
    }, {
        '$unwind': {
            'path': '$top_words', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$group': {
            '_id': '$top_words', 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 400
    }, {
        '$addFields': {
            'word_classification': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$in': [
                                    '$_id', [
                                        "helpful", "friendly", "clean", "nice", "comfortable", "great", "good", "excellent", "positive", "lovely", "perfect", "comfy", "quiet", "amazing", "nice", "spacious", "beautiful", "right", "welcoming", "recommend", "wonderful", "convenient", "polite", "liked", "fine", "attentive", "top"
                                    ]
                                ]
                            }, 
                            'then': 'positive'
                        }, {
                            'case': {
                                '$in': [
                                    '$_id', [
                                        "not", "negative", "expensive", "noisy", "poor", "didn't", "noise", "bad", "slow", "problem"
                                    ]
                                ]
                            }, 
                            'then': 'negative'
                        }
                    ], 
                    'default': 'neutral'
                }
            }
        }
    }, {
        '$group': {
            '_id': '$word_classification', 
            'word_classifications': {
                '$addToSet': '$_id'
            }
        }
    }, {
        '$project': {
            '_id': 1, 
            'word_classifications': {
                '$reduce': {
                    'input': '$word_classifications', 
                    'initialValue': '', 
                    'in': {
                        '$concat': [
                            '$$value', '|', '$$this'
                        ]
                    }
                }
            }
        }
    }
]

wc_result = list(db.aggregate(word_count_pipeline))
pos = [entry['word_classifications'] for entry in wc_result if entry['_id'] == 'positive'][0]
neg = [entry['word_classifications'] for entry in wc_result if entry['_id'] == 'negative'][0]
pos = pos.strip('|')
neg = neg.strip('|')
# print(pos)


sentiment_pipeline = [
    {
        '$match': {
            '$expr': {
                '$and': [
                    {'$gte': [{'$toDouble': '$Age'}, 20]},
                    {'$lte': [{'$toDouble': '$Age'}, 40]}
                ]
            }
        }
    },
    {
        '$project': {
            '_id': 1,
            'Nationality': '$Nationality',
            'Age': '$Age',
            'SRHighFloor': '$SRHighFloor',
            'SRLowFloor': '$SRLowFloor',
            'SRAccessibleRoom': '$SRAccessibleRoom',
            'SRMediumFloor': '$SRMediumFloor',
            'SRBathtub': '$SRBathtub',
            'SRShower': '$SRShower',
            'CustomerReview': {'$toString':'$CustomerReview'}
        }
    },
    {
        '$addFields': {
            'sentiment': {
                '$cond': {
                    'if': {
                        '$regexMatch': {
                            'input': '$CustomerReview',
                            'regex': pos,
                            'options': 'i'
                        }
                    },
                    'then': 'positive',
                    'else': {
                        '$cond': {
                            'if': {
                                '$regexMatch': {
                                    'input': '$CustomerReview',
                                    'regex': neg,
                                    'options': 'i'
                                }
                            },
                            'then': 'negative',
                            'else': 'neutral'
                        }
                    }
                }
            }
        }
    },
    {
        '$project': {
            '_id': 1,
            'Nationality': '$Nationality',
            'Age': '$Age',
            'SRHighFloor': '$SRHighFloor',
            'SRLowFloor': '$SRLowFloor',
            'SRAccessibleRoom': '$SRAccessibleRoom',
            'SRMediumFloor': '$SRMediumFloor',
            'SRBathtub': '$SRBathtub',
            'SRShower': '$SRShower',
            'CustomerReview': '$CustomerReview',
            'Sentiment': '$sentiment'
        }
    },
    {
        '$group': {
            '_id': '$Nationality',
            'pos_count': {'$sum': {'$cond': [{'$eq': ['$Sentiment', 'positive']}, 1, 0]}},
            'neg_count': {'$sum': {'$cond': [{'$eq': ['$Sentiment', 'negative']}, 1, 0]}},
            'neut_count': {'$sum': {'$cond': [{'$eq': ['$Sentiment', 'neutral']}, 1, 0]}}
        }
    }
]

sentiment_result = list(db.aggregate(sentiment_pipeline))

nationalities = [entry['_id'] for entry in sentiment_result]
pos_count = [entry['pos_count'] for entry in sentiment_result]
neg_count = [entry['neg_count'] for entry in sentiment_result]
neut_count = [entry['neut_count'] for entry in sentiment_result]


fig_sentiment = go.Figure()

fig_sentiment.add_trace(go.Bar(x=nationalities, y=pos_count, name='Positive', marker_color='green'))
fig_sentiment.add_trace(go.Bar(x=nationalities, y=neut_count, name='Neutral', marker_color='blue'))
fig_sentiment.add_trace(go.Bar(x=nationalities, y=neg_count, name='Negative', marker_color='red'))


# Update the layout
fig_sentiment.update_layout(
    title='Geospatial Sentiment Analysis',
    xaxis_title='Nationality',
    yaxis_title='Count',
    barmode='group'  # Grouped bar chart
)

st.title("Geospatial Sentiment Analysis")

st.plotly_chart(fig_sentiment, use_container_width=True)

# for w in sentiment_result:
#     try:
#         print(w)
#     except:
#         print("a")