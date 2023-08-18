import streamlit as st

from css import all_css
import graphistry, pandas as pd, numpy as np
from components import GraphistrySt
import os
from components import cfgMaker, createDep, ValidAction, chart_functions
import subprocess
import altair as alt
from streamlit_echarts import st_echarts
import json
from streamlit_apexjs import st_apexcharts

page_title_str = "Graph dashboard"
st.set_page_config(
    layout="wide",  # Can be "centered" or "wide". In the future also "dashboard", etc.
    initial_sidebar_state="auto",  # Can be "auto", "expanded", "collapsed"
    page_title=page_title_str,  # String or None. Strings get appended with "• Streamlit".
    page_icon=os.environ.get('FAVICON_URL', 'https://hub.graphistry.com/pivot/favicon/favicon.ico'),
    # String, anything supported by st.image, or None.
)

st.markdown(
    '''
    <style>
    .streamlit-expanderHeader {
        background-color: white;
        color: black; # Adjust this for expander header color
    }
    .streamlit-expanderContent {
        background-color: white;
        color: black; # Expander content color
    }
    </style>
    ''',
    unsafe_allow_html=True
)

st.markdown("""
<style>
div[data-testid="metric-container"] {
   background-color: rgba(28, 131, 225, 0.1);
   border: 1px solid rgba(28, 131, 225, 0.1);
   padding: 5% 5% 5% 10%;
   border-radius: 5px;
   color: rgb(30, 103, 119);
   overflow-wrap: break-word;
   
}

/* breakline for metric text         */
div[data-testid="metric-container"] > label[data-testid="stMetricLabel"] > div {
   overflow-wrap: break-word;
   white-space: break-spaces;
   color: red;
}

</style>
"""
            , unsafe_allow_html=True)


def run():
    run_all()


def dataCSV(csv1, csv2):
    df1 = pd.read_csv(csv1)
    df2 = pd.read_csv(csv2)
    df2 = df2.rename(columns={'target': 'url'})

    groupAnchor = df2.groupby('url')['text'].nunique().reset_index(name='nb_anchors_unique')

    df2.pop('text')
    df2.pop('source')
    df2.pop('nofollow')
    df2.pop('disallow')

    merge = pd.merge(df1, df2.drop_duplicates(subset=['url']), on='url', how='left', suffixes=('', ''))

    merge = pd.merge(merge, groupAnchor, on='url', how='left')

    return merge


def custom_css():
    all_css()
    st.markdown(
        """<style>

        </style>""", unsafe_allow_html=True)


@st.cache_data()
def run_filters(file, links_type, urls_file):
    if links_type:
        links = file.drop_duplicates(subset=['target'])
    else:
        links = file

    # Read the file containing pagerank
    urls_df = pd.read_csv(urls_file)

    # Merge links with urls_df to get pagerank
    links = pd.merge(links, urls_df[['url', 'pagerank']], left_on='target', right_on='url', how='left')

    links['pagerank'].fillna(0, inplace=True)
    links["label"] = links.pagerank.map(lambda v: "Pagerank: %d" % int(v))

    graph_url = \
        graphistry. \
            edges(links) \
            .bind(source="source", destination="target") \
            .bind(point_size="pagerank", edge_title="pagerank") \
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
        lang = st.sidebar.checkbox("Detect Language")
        surfer = st.sidebar.radio("Choose a surfer model", ('basic', 'advanced'))

        link_unique = st.sidebar.checkbox("Link unique for Visualization", key="disabled")

        dataConfig = [text_url, values, depth, lang, surfer]

        st.markdown("""
            <style>
                table.dataframe th, table.dataframe td {
                    background-color: white !important;
                }
            </style>
            """, unsafe_allow_html=True)

        if text_url:
            button_clicked = False
            default_display = 'dataframe'

            root = createDep().pathProject(text_url)
            slugName = createDep().url_to_name(text_url)
            urls_file = root + '/_urls.csv'
            print(urls_file)

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

            total_pages = len(dataFrame['url'].unique())

            dataFrame_links = pd.read_csv(f"{root}/_links.csv")

            total_relations = len(dataFrame_links['source'])

            lang_counts = dataFrame['content_lang'].value_counts()

            if not lang_counts.empty:
                most_common_lang = lang_counts.index[0]
                most_common_lang_count = lang_counts.iloc[0]
            else:
                most_common_lang = None
                most_common_lang_count = None

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(label="Total Pages", value=total_pages)
            with col2:
                st.metric(label="Total Relations", value=total_relations)
            with col3:
                if most_common_lang:
                    st.metric(label=f"Language : {most_common_lang.upper()}", value=f"{most_common_lang_count} ")
                else:
                    st.metric(label="Language : N/A", value="N/A")

            col5, col6 = st.columns(2)

            with col5:
                expander = st.expander("Graphs")
                with expander:
                    col1, col2 = st.columns(2)
                    show_general = col1.button("General")
                    show_graph = col2.button("Other Graph")

            with col6:
                expander1 = st.expander("Datas")
                with expander1:
                    col3, col4 = st.columns(2)
                    show_dataframe = col3.button("DataFrame")
                    show_visualization = col4.button("Visualization")

            def display_graph_content():
                with st.container():
                    col1, col2 = st.columns(2)
                    with col1:
                        options, series = chart_functions().status_code_apex(dataFrame)
                        st.header("Response Code Distribution")
                        st_apexcharts(options, series, 'donut', '600')
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

                        options, series = chart_functions().http_status_code_by_depth_chart_apex(level,
                                                                                                 response_code_depth_200,
                                                                                                 df,
                                                                                                 response_code_depth_400,
                                                                                                 response_code_depth_500)
                        st.header("HTTP Status Code by Depth Chart")
                        st_apexcharts(options, series, 'bar', '600')
                with st.container():
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        options, series = chart_functions().https_distribution_apex(dataFrame)
                        st.header("HTTPS Distribution")
                        st_apexcharts(options, series, 'radialBar', '600')

                    with col2:
                        options, series = chart_functions().language_distribution_apex(dataFrame)
                        st.header("Language Distribution")
                        st_apexcharts(options, series, 'donut', '600')

                    with col3:
                        options, series = chart_functions().latency_distribution_apex(dataFrame)
                        st.header("Latency Distribution")
                        st_apexcharts(options, series, "donut", "600")

                with st.container():
                    col1, col2 = st.columns(2)
                    with col1:
                        options, series = chart_functions().links_per_depth_apex(dataFrame)
                        st.header("Links per Depth")
                        st_apexcharts(options, series, "bar", "600")
                with st.container():
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        options, series = chart_functions().title_distribution_apex(dataFrame)
                        st.header("Title Distribution")
                        st_apexcharts(options, series, 'radialBar', '600')

                    with col2:
                        options, series = chart_functions().h1_distribution_apex(dataFrame)
                        st.header("H1 Tag Distribution")
                        st_apexcharts(options, series, 'radialBar', '600')

                    with col3:
                        options, series = chart_functions().meta_description_distribution_apex(dataFrame)
                        st.header("Meta Description Distribution")
                        st_apexcharts(options, series, 'radialBar', '600')

            if show_dataframe:
                button_clicked = True
                st.dataframe(dataFrame, height=600)

            # Compute filter pipeline (with auto-caching based on filter setting inputs)
            # Selective mark these as URL params as well
            if show_visualization:
                button_clicked = True
                filter_pipeline_result = run_filters(dataFrame_links, link_unique, urls_file)

                # Render main viz area based on computed filter pipeline results and sidebar settings
                main_area(**filter_pipeline_result)
            if show_general:
                button_clicked = True
                display_graph_content()

            if show_graph:
                button_clicked = True
                with st.container():
                    col1, col2 = st.columns(2)
                    with col1:
                        options, series = chart_functions().wordcount_distribution_apex(dataFrame)
                        st.header("Word Count Distribution")
                        st_apexcharts(options, series, 'bar', '600')
                    with col2:
                        options, series = chart_functions().pagerank_distribution_apex(dataFrame)
                        st.header("PageRank Distribution")
                        st_apexcharts(options, series, 'bar', '600')
            if not button_clicked and default_display == 'dataframe':
                show_dataframe = True
                st.dataframe(dataFrame, height=600)
        else:
            default_display = None




    except Exception as exn:
        st.write('Error loading dashboard')
        st.write(exn)


run_all()
