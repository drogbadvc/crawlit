import streamlit as st

from css import all_css
import graphistry, pandas as pd, numpy as np
from components import GraphistrySt
import os
from components import cfgMaker, createDep, ValidAction, DataPandas
import subprocess
import altair as alt
from streamlit_echarts import st_echarts
import json
from collections import Counter

page_title_str = "Graph dashboard"
st.set_page_config(
    layout="wide",  # Can be "centered" or "wide". In the future also "dashboard", etc.
    initial_sidebar_state="auto",  # Can be "auto", "expanded", "collapsed"
    page_title=page_title_str,  # String or None. Strings get appended with "• Streamlit".
    page_icon=os.environ.get('FAVICON_URL', 'https://hub.graphistry.com/pivot/favicon/favicon.ico'),
    # String, anything supported by st.image, or None.
)


def run():
    run_all()


@st.cache_data()
def dataCSV(csv1, csv2):
    df1 = pd.read_csv(csv1)
    df2 = pd.read_csv(csv2)
    df2 = df2.rename(columns={'target': 'url'})

    groupAnchor = df2.groupby('url')['text'].nunique().reset_index(name='nb_anchors_unique')

    df2.pop('text')
    df2.pop('source')
    df2.pop('nofollow')
    df2.pop('disallow')

    merge = pd.merge(df1, df2.drop_duplicates(subset=['url']), on='url', how='left')

    return merge


def custom_css():
    all_css()
    st.markdown(
        """<style>

        </style>""", unsafe_allow_html=True)


@st.cache_data()
def status_code(dataframe):
    # df2 = dataframe.loc[dataframe["response_code"].between("200", "299")]
    # print(df2)
    response_200 = DataPandas().response_code(200, dataframe)
    response_301 = DataPandas().response_code(301, dataframe)
    response_404 = DataPandas().response_code(404, dataframe)
    response_500 = DataPandas().response_code(500, dataframe)

    options = {
        "color": ['#8269b2', '#ffeaa7', '#e17055', '#d63031'],
        "tooltip": {"trigger": "item"},
        "legend": {"top": "5%", "left": "center"},
        "series": [
            {
                "name": "Response_code",
                "type": "pie",
                "radius": ['45%', '60%'],
                "avoidLabelOverlap": False,
                "itemStyle": {
                    "borderRadius": 10,
                    "borderColor": "#fff",
                    "borderWidth": 1,
                },
                "label": {"show": False, "position": "center"},
                "emphasis": {
                    "label": {"show": True, "fontSize": "40", "fontWeight": "bold"}
                },
                "labelLine": {"show": False},
                "data": [
                    {"value": response_200, "name": "200"},
                    {"value": response_301, "name": "301"},
                    {"value": response_404, "name": "404"},
                    {"value": response_500, "name": "500"}
                ],
            }
        ],
    }

    return options


@st.cache_data()
def https_distribution(dataframe):
    """Returns the distribution of URLs based on their protocol (HTTP or HTTPS)"""
    https_count = dataframe[dataframe['url'].str.startswith('https://')].shape[0]
    http_count = dataframe[dataframe['url'].str.startswith('http://')].shape[0]

    options = {
        "color": ['#82ca9d', '#ff9f40'],
        "tooltip": {"trigger": "item"},
        "legend": {"top": "5%", "left": "center"},
        "series": [
            {
                "name": "Protocols",
                "type": "pie",
                "radius": ['45%', '60%'],
                "avoidLabelOverlap": False,
                "itemStyle": {
                    "borderRadius": 10,
                    "borderColor": "#fff",
                    "borderWidth": 1,
                },
                "label": {"show": False, "position": "center"},
                "emphasis": {
                    "label": {"show": True, "fontSize": "40", "fontWeight": "bold"}
                },
                "labelLine": {"show": False},
                "data": [
                    {"value": https_count, "name": "HTTPS"},
                    {"value": http_count, "name": "HTTP"}
                ],
            }
        ],
    }

    return options


@st.cache_data()
def links_per_depth(dataframe):
    """Returns the number of links per depth level"""
    levels = list(range(1, 11))  # Depth levels from 1 to 10
    counts = []

    for lvl in levels:
        count = dataframe[dataframe['level'] == lvl].shape[0]
        counts.append(count)

    options = {
        "color": ['#6495ED'],  # Color for bars
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "shadow"
            }
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": [str(lvl) for lvl in levels]
        },
        "yAxis": {
            "type": "value"
        },
        "series": [
            {
                "name": "Number of Links",
                "type": "bar",
                "data": counts
            }
        ]
    }

    return options


@st.cache_data()
def title_distribution(dataframe):
    """Returns the percentage of pages with and without titles."""
    total = dataframe.shape[0]
    with_title = dataframe[dataframe['title'].notnull()].shape[0]
    without_title = total - with_title

    options = {
        "color": ['#34a853', '#ea4335'],  # Colors for with and without titles
        "tooltip": {
            "trigger": "item",
            "formatter": "{a} <br/>{b}: {c} ({d}%)"
        },
        "legend": {
            "top": "5%",
            "left": "center",
            "data": ["With Title", "Without Title"]
        },
        "series": [
            {
                "name": "Titles",
                "type": "pie",
                "radius": ['45%', '60%'],
                "avoidLabelOverlap": False,
                "label": {
                    "show": True,
                    "position": "center"
                },
                "emphasis": {
                    "label": {
                        "show": True,
                        "fontSize": "20",
                        "fontWeight": "bold"
                    }
                },
                "labelLine": {
                    "show": False
                },
                "data": [
                    {"value": with_title, "name": "With Title"},
                    {"value": without_title, "name": "Without Title"}
                ],
            }
        ],
    }

    return options


@st.cache_data()
def h1_distribution(dataframe):
    """Returns the percentage of pages with and without h1."""
    total = dataframe.shape[0]
    with_h1 = dataframe[dataframe['h1'].notnull()].shape[0]
    without_h1 = total - with_h1

    options = {
        "color": ['#4285f4', '#fbbc05'],  # Colors for with and without h1
        "tooltip": {
            "trigger": "item",
            "formatter": "{a} <br/>{b}: {c} ({d}%)"
        },
        "legend": {
            "top": "5%",
            "left": "center",
            "data": ["With H1", "Without H1"]
        },
        "series": [
            {
                "name": "H1 Tags",
                "type": "pie",
                "radius": ['45%', '60%'],
                "avoidLabelOverlap": False,
                "label": {
                    "show": True,
                    "position": "center"
                },
                "emphasis": {
                    "label": {
                        "show": True,
                        "fontSize": "20",
                        "fontWeight": "bold"
                    }
                },
                "labelLine": {
                    "show": False
                },
                "data": [
                    {"value": with_h1, "name": "With H1"},
                    {"value": without_h1, "name": "Without H1"}
                ],
            }
        ],
    }

    return options


@st.cache_data()
def meta_description_distribution(dataframe):
    """Returns the percentage of pages with and without meta description."""
    total = dataframe.shape[0]
    with_meta_description = \
        dataframe[dataframe['meta_description'].notnull() & dataframe['meta_description'] != ''].shape[0]
    without_meta_description = total - with_meta_description

    options = {
        "color": ['#34a853', '#ea4335'],  # Colors for with and without meta description
        "tooltip": {
            "trigger": "item",
            "formatter": "{a} <br/>{b}: {c} ({d}%)"
        },
        "legend": {
            "top": "5%",
            "left": "center",
            "data": ["With Meta Description", "Without Meta Description"]
        },
        "series": [
            {
                "name": "Meta Descriptions",
                "type": "pie",
                "radius": ['45%', '60%'],
                "avoidLabelOverlap": False,
                "label": {
                    "show": True,
                    "position": "center"
                },
                "emphasis": {
                    "label": {
                        "show": True,
                        "fontSize": "20",
                        "fontWeight": "bold"
                    }
                },
                "labelLine": {
                    "show": False
                },
                "data": [
                    {"value": with_meta_description, "name": "With Meta Description"},
                    {"value": without_meta_description, "name": "Without Meta Description"}
                ],
            }
        ],
    }

    return options


@st.cache_data()
def wordcount_distribution(dataframe):
    # Segment the dataframe based on wordcount
    range_0_500 = dataframe[(dataframe['wordcount'] >= 0) & (dataframe['wordcount'] <= 500)].shape[0]
    range_500_1000 = dataframe[(dataframe['wordcount'] > 500) & (dataframe['wordcount'] <= 1000)].shape[0]
    range_1000_2000 = dataframe[(dataframe['wordcount'] > 1000) & (dataframe['wordcount'] <= 2000)].shape[0]
    range_2000_plus = dataframe[dataframe['wordcount'] > 2000].shape[0]

    categories = ["0-500", "500-1000", "1000-2000", "2000+"]
    values = [range_0_500, range_500_1000, range_1000_2000, range_2000_plus]

    options = {
        "title": {
            "text": "Word Count Distribution"
        },
        "tooltip": {},
        "xAxis": {
            "type": "category",
            "data": categories
        },
        "yAxis": {
            "type": "value"
        },
        "series": [{
            "data": values,
            "type": "bar",
            "showBackground": True,
            "backgroundStyle": {
                "color": 'rgba(180, 180, 180, 0.2)'
            }
        }]
    }

    return options


def pagerank_distribution(dataframe):
    max_pagerank = dataframe['pagerank'].max()
    rounded_pageranks = [round(10 * (pr / max_pagerank)) for pr in dataframe['pagerank']]

    counted_pageranks = Counter(rounded_pageranks)

    options = {
        "title": {
            "text": "Pageranks distribution"
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "shadow"
            },
            "formatter": "{a} <br/>{b} : {c} URLs"
        },
        "xAxis": {
            "type": "category",
            "data": list(range(10, 0, -1))  # Invert the order for x-axis
        },
        "yAxis": {
            "type": "value"
        },
        "series": [{
            "name": "Number of URLs",
            "data": [counted_pageranks.get(i, 0) for i in range(10, 0, -1)],  # Invert the order for data
            "type": "bar"
        }]
    }

    return options


@st.cache_data()
def run_filters(file, links_type):
    if links_type:
        links = pd.read_csv(file).drop_duplicates(subset=['target'])
    else:
        links = pd.read_csv(file)

    links["label"] = links.weight.map(lambda v: "#Meetings: %d" % v)

    graph_url = \
        graphistry. \
            edges(links) \
            .bind(source="source", destination="target") \
            .bind(point_size="weight", edge_title="weight") \
            .settings(url_params={'linLog': True, 'strongGravity': False, 'dissuadeHubs': True, 'play': 4000}) \
            .plot(render=False)
    return {'edges_df': links, 'graph_url': graph_url}


def main_area(edges_df, graph_url):
    GraphistrySt().render_url(graph_url)


def run_all():
    custom_css()

    try:

        text_url = st.sidebar.text_input("Enter some text 👇", placeholder="https://www.google.com/")
        values = st.sidebar.slider('Concurrent Requests', 0, 50, 5)
        depth = st.sidebar.slider('Maximum depth', 0, 100, 5)
        link_unique = st.sidebar.checkbox("Link unique for Visualization", key="disabled")

        dataConfig = [text_url, values, depth]

        if text_url:
            button_clicked = False
            default_display = 'dataframe'
            col1, col2, col3, col4 = st.columns(4)

            show_dataframe = col1.button("DataFrame")
            show_visualization = col2.button("Visualization")
            show_general = col3.button("General")
            show_graph = col4.button("Other Graph")

            root = createDep().pathProject(text_url)
            slugName = createDep().url_to_name(text_url)
            urls_file = root + '/_urls.csv'

            if not (ValidAction().projectIsset(urls_file)):
                ValidAction().checkCrawlCache(slugName)
                createDep().mkdir(text_url)

                cfgMaker().cfg(dataConfig, root)
                process = subprocess.Popen(
                    f"python3.7 crowl/crowl.py --conf {root}/config.ini --resume crowl/data/{createDep().url_to_name(text_url)}/",
                    shell=True)
                out, err = process.communicate()
                errcode = process.returncode

            dataFrame = dataCSV(f"{urls_file}", f"{root}/_links.csv")
            dataFrame['response_code'] = dataFrame['response_code'].astype(str)
            cols = dataFrame.columns.tolist()
            cols.remove('pagerank')

            cols.insert(1, 'pagerank')
            dataFrame = dataFrame[cols]

            def display_graph_content():
                with st.container():
                    col1, col2 = st.columns(2)
                    with col1:
                        st.header("HTTP Status Code Chart")
                        st_echarts(
                            options=status_code(dataFrame), height="300px",
                        )
                    with col2:
                        depth_by_code = dataFrame.groupby(["level", "response_code"]).size().reset_index(name="count")
                        level = depth_by_code['level'].unique()
                        response_code_depth_200 = depth_by_code.query("response_code == '200'")
                        response_code_depth_300 = depth_by_code.query("'300' <= response_code <= '399'")

                        lvl_list = response_code_depth_300['level'].tolist()
                        append_data = []
                        for lvl in level:
                            if lvl_list.count(lvl) == 0:
                                append_data.append({'level': lvl, 'response_code': '301', 'count': '0'})

                        # print(append_data)
                        df = response_code_depth_300.append([{'level': '0', 'response_code': '301', 'count': '0'},
                                                             {'level': '1', 'response_code': '301', 'count': '0'}],
                                                            ignore_index=True)
                        # print(df)
                        df['level'] = df['level'].astype(str)

                        df.sort_values(by=['level'], inplace=True)

                        response_code_depth_400 = depth_by_code.query("response_code == '400'")
                        response_code_depth_500 = depth_by_code.query("response_code == '500'")

                        st.header("HTTP Status Code by Depth Chart")
                        options = {
                            "color": ['#797bf2', '#ffeaa7', '#e17055', '#d63031'],
                            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
                            "legend": {
                                "data": ["200", "300", "400", "500"]
                            },
                            "grid": {"bottom": 100},
                            "yAxis": {"type": "value"},
                            "xAxis": {
                                "type": "category",
                                "data": level.tolist(),
                            },
                            "series": [
                                {
                                    "name": "200",
                                    "type": "bar",
                                    "stack": "total",
                                    "label": {"show": True},
                                    "emphasis": {"focus": "series"},
                                    "data": response_code_depth_200['count'].tolist(),
                                },
                                {
                                    "name": "300",
                                    "type": "bar",
                                    "stack": "total",
                                    "label": {"show": True},
                                    "emphasis": {"focus": "series"},
                                    "data": df['count'].tolist(),
                                },
                                {
                                    "name": "400",
                                    "type": "bar",
                                    "stack": "total",
                                    "label": {"show": True},
                                    "emphasis": {"focus": "series"},
                                    "data": response_code_depth_400['count'].tolist(),
                                },
                                {
                                    "name": "500",
                                    "type": "bar",
                                    "stack": "total",
                                    "label": {"show": True},
                                    "emphasis": {"focus": "series"},
                                    "data": response_code_depth_500['count'].tolist(),
                                }
                            ],
                        }

                        st_echarts(options=options, height="300px")
                with st.container():
                    col1, col2 = st.columns(2)
                    with col1:
                        st.header("HTTP/HTTPS Distribution")
                        st_echarts(
                            options=https_distribution(dataFrame), height="300px",
                        )
                    with col2:
                        st.header("Links per Depth")
                        st_echarts(
                            options=links_per_depth(dataFrame), height="400px",
                        )
                with st.container():
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.header("Title Distribution")
                        st_echarts(
                            options=title_distribution(dataFrame), height="300px",
                        )
                    with col2:
                        st.header("H1 Distribution")
                        st_echarts(
                            options=h1_distribution(dataFrame), height="300px",
                        )
                    with col3:
                        st.header("Meta Description Distribution")
                        st_echarts(
                            options=meta_description_distribution(dataFrame), height="300px",
                        )

            if show_dataframe:
                button_clicked = True
                st.dataframe(dataFrame, height=600)

            # Compute filter pipeline (with auto-caching based on filter setting inputs)
            # Selective mark these as URL params as well
            if show_visualization:
                button_clicked = True
                filter_pipeline_result = run_filters(root + '/_links.csv', link_unique)

                # Render main viz area based on computed filter pipeline results and sidebar settings
                main_area(**filter_pipeline_result)
            if show_general:
                button_clicked = True
                display_graph_content()

            if show_graph:
                button_clicked = True
                with st.container():
                    st.header("Word Count Distribution")
                    st_echarts(
                        options=wordcount_distribution(dataFrame), height="300px",
                    )
                    st.header("PageRank Distribution")
                    st_echarts(
                        options=pagerank_distribution(dataFrame), height="300px",
                    )
            if not button_clicked and default_display == 'dataframe':
                show_dataframe = True
                st.dataframe(dataFrame, height=600)
        else:
            default_display = None




    except Exception as exn:
        st.write('Error loading dashboard')
        st.write(exn)


run_all()
