import graphistry, os, streamlit as st, streamlit.components.v1 as components
from graphistry import PyGraphistry

from util import getChild
logger = getChild(__name__)

logger.debug('Using graphistry version: %s', graphistry.__version__)


class GraphistrySt:

    def __init__(self, overrides={}):
        graphistry.register(api=3, personal_key_id='CZSF3TG59H',
                            personal_key_secret='IEYZ2FWQ6FW436FR')

    def render_url(self, url):
        if self.test_login():
            logger.debug('rendering main area, with url: %s', url)
            #iframe = '<iframe src="' + url + '", height="800", width="100%" style="position: absolute" allow="fullscreen"></iframe>'
            #st.markdown(iframe, unsafe_allow_html=True)
            components.iframe(
                src=url,
                height=800,
                scrolling=True
            )

    def plot(self, g):
        if PyGraphistry._is_authenticated:
            url = g.plot(as_files=True, render=False)  # TODO: Remove as_files=True when becomes default
            self.render_url(url)
        else:

            st.markdown("""
                Graphistry not authenticated. Did you set credentials in docker/.env based on envs/graphistry.env ?
            """)

    def test_login(self, verbose=True):
        try:
            graphistry.register()
            return True
        except:  # noqa: E722
            if verbose:
                st.write(Exception("""Not logged in for Graphistry plots:
                    Get free GPU account at graphistry.com/get-started and
                    plug into src/docker/.env using template at envs/graphistry.env"""))
            return False


GraphistrySt()
